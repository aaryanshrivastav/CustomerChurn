evaluate_churn_model <- function(models, test_df, cfg) {
  tryCatch(
    {
      pred_probs <- stats::predict(
        models$logistic_model,
        newdata = test_df,
        type = "response"
      )

      # Confusion matrix @ threshold
      pred_class <- as.integer(pred_probs >= cfg$training$churn_threshold)
      cm <- table(actual = test_df$churned, predicted = pred_class)

      # Handle edge case where all predictions are 0 or 1
      if (nrow(cm) < 2 || ncol(cm) < 2) {
        return(list(
          auc = NA_real_,
          sensitivity = NA_real_,
          specificity = NA_real_,
          precision = NA_real_,
          f1_score = NA_real_,
          confusion_matrix = cm,
          error = "Insufficient variation in predictions"
        ))
      }

      sensitivity <- cm[2, 2] / sum(cm[2, ])
      specificity <- cm[1, 1] / sum(cm[1, ])
      precision <- cm[2, 2] / sum(cm[, 2])

      f1 <- if (!is.na(sensitivity) && !is.na(precision) && (sensitivity + precision) > 0) {
        2 * (precision * sensitivity) / (precision + sensitivity)
      } else {
        NA_real_
      }

      # Calculate AUC if enough variation
      if (length(unique(pred_probs)) > 1 && length(unique(test_df$churned)) > 1) {
        suppressWarnings({
          roc_obj <- pROC::roc(test_df$churned, pred_probs, quiet = TRUE)
          auc_score <- pROC::auc(roc_obj)
        })
      } else {
        auc_score <- NA_real_
      }

      list(
        auc = as.numeric(auc_score),
        sensitivity = as.numeric(sensitivity),
        specificity = as.numeric(specificity),
        precision = as.numeric(precision),
        f1_score = as.numeric(f1),
        confusion_matrix = cm
      )
    },
    error = function(e) {
      warning("Churn model evaluation failed: ", e$message)
      list(
        auc = NA_real_,
        sensitivity = NA_real_,
        specificity = NA_real_,
        precision = NA_real_,
        f1_score = NA_real_,
        confusion_matrix = NA,
        error = e$message
      )
    }
  )
}

evaluate_survival_model <- function(models, test_df) {
  tryCatch(
    {
      pred_risk <- stats::predict(
        models$cox_model,
        newdata = test_df,
        type = "risk"
      )

      # Concordance index
      concordance <- survival::concordanceC(
        test_df$duration,
        test_df$event,
        pred_risk
      )

      list(
        concordance_index = concordance$concordance,
        se = concordance$se
      )
    },
    error = function(e) {
      warning("Survival model evaluation failed: ", e$message)
      list(
        concordance_index = NA_real_,
        se = NA_real_,
        error = e$message
      )
    }
  )
}

save_model_report <- function(models, test_df, cfg) {
  tryCatch(
    {
      dir.create("logs", showWarnings = FALSE)

      churn_metrics <- evaluate_churn_model(models, test_df, cfg)
      surv_metrics <- evaluate_survival_model(models, test_df)

      report <- list(
        model_run_ts = as.character(Sys.time()),
        churn_auc = churn_metrics$auc,
        churn_sensitivity = churn_metrics$sensitivity,
        churn_specificity = churn_metrics$specificity,
        churn_precision = churn_metrics$precision,
        churn_f1 = churn_metrics$f1_score,
        survival_concordance = surv_metrics$concordance_index,
        test_set_size = nrow(test_df),
        notes = "Model performance on test set"
      )

      if (!requireNamespace("jsonlite", quietly = TRUE)) {
        warning("jsonlite not available; skipping JSON report")
        return(report)
      }

      cat(jsonlite::toJSON(report, pretty = TRUE), file = "logs/model_report.json")

      report
    },
    error = function(e) {
      warning("Model report generation failed: ", e$message)
      list(error = e$message)
    }
  )
}
