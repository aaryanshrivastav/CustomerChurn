safe_csv_load <- function(csv_path, fallback_to_synthetic = TRUE, seed = 42) {
  tryCatch(
    {
      raw <- read.csv(csv_path, stringsAsFactors = FALSE)
      validate_telco_schema(raw)
      load_customers_from_csv(csv_path, seed = seed)
    },
    error = function(e) {
      if (isTRUE(fallback_to_synthetic)) {
        warning("CSV load failed: ", e$message, ". Falling back to synthetic data.")
        generate_synthetic_customers(n_customers = 5000, seed = seed)
      } else {
        stop("CSV load failed and fallback disabled: ", e$message)
      }
    }
  )
}

safe_predict_batch <- function(models, new_df, cfg) {
  tryCatch(
    predict_customer_batch(models, new_df, cfg),
    error = function(e) {
      warning("Prediction failed for batch: ", e$message)
      data.frame(
        customer_id = new_df$customer_id,
        churn_probability = 0.5,
        expected_time_to_churn = NA,
        predicted_clv = NA,
        stringsAsFactors = FALSE
      )
    }
  )
}

safe_db_operation <- function(operation_name, expr) {
  tryCatch(
    expr,
    error = function(e) {
      warning("Database operation '", operation_name, "' failed: ", e$message)
      invisible(NULL)
    }
  )
}
