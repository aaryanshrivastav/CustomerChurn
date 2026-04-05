run_prediction_pipeline <- function(conn, customer_df, models, cfg) {
  if (nrow(customer_df) == 0) {
    log_event("Pipeline warning", "Empty customer dataframe passed to prediction pipeline")
    return(customer_df)
  }

  tryCatch(
    {
      # Validate input
      validate_dataframe_structure(
        customer_df,
        c("customer_id", "age", "tenure_months", "monthly_charges", "is_active")
      )

      # Generate predictions
      predictions <- safe_predict_batch(models, customer_df, cfg)
      validate_predictions(predictions)

      decisions <- recommend_interventions(predictions, cfg)

      run_ts <- as.character(Sys.time())

      prediction_table <- data.frame(
        customer_id = predictions$customer_id,
        churn_prob_logit = predictions$churn_prob_logit,
        churn_prob_rf = predictions$churn_prob_rf,
        churn_probability = predictions$churn_probability,
        model_run_ts = run_ts,
        stringsAsFactors = FALSE
      )

      survival_table <- data.frame(
        customer_id = predictions$customer_id,
        survival_risk = predictions$survival_risk,
        expected_time_to_churn = predictions$expected_time_to_churn,
        model_run_ts = run_ts,
        stringsAsFactors = FALSE
      )

      clv_table <- data.frame(
        customer_id = predictions$customer_id,
        predicted_clv = predictions$predicted_clv,
        model_run_ts = run_ts,
        stringsAsFactors = FALSE
      )

      decision_table <- data.frame(
        customer_id = decisions$customer_id,
        expected_loss = decisions$expected_loss,
        recommended_action = decisions$recommended_action,
        expected_benefit = decisions$expected_benefit,
        loss_discount = decisions$loss_discount,
        loss_retention_call = decisions$loss_retention_call,
        loss_no_action = decisions$loss_no_action,
        model_run_ts = run_ts,
        stringsAsFactors = FALSE
      )

      # Persist predictions
      safe_db_operation("Append predictions", {
        append_predictions(conn, prediction_table)
      })

      safe_db_operation("Append survival estimates", {
        append_survival_estimates(conn, survival_table)
      })

      safe_db_operation("Append CLV values", {
        append_clv_values(conn, clv_table)
      })

      safe_db_operation("Append intervention decisions", {
        append_intervention_decisions(conn, decision_table)
      })

      # Log prediction events
      for (i in seq_len(min(nrow(predictions), 5))) {
        log_prediction(conn, predictions$customer_id[i], predictions[i, ])
      }

      log_event("Pipeline success", sprintf("Scored %d customers", nrow(predictions)))

      merge(
        predictions,
        decisions,
        by = "customer_id",
        all.x = TRUE,
        sort = FALSE
      )
    },
    error = function(e) {
      log_event("Pipeline error", e$message, is_error = TRUE)
      data.frame(
        customer_id = customer_df$customer_id,
        churn_probability = 0.5,
        expected_loss = NA,
        recommended_action = "no_action",
        stringsAsFactors = FALSE
      )
    }
  )
}
