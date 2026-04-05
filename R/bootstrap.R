bootstrap_system <- function(cfg, force_reload = FALSE) {
  conn <- get_db_connection(cfg)
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  setup_logging()
  log_event("Bootstrap system", "Starting initialization")

  initialize_database(conn)

  if (isTRUE(force_reload)) {
    log_event("Force reload", "Clearing all tables")
    DBI::dbExecute(conn, "DELETE FROM intervention_decisions")
    DBI::dbExecute(conn, "DELETE FROM clv_values")
    DBI::dbExecute(conn, "DELETE FROM survival_estimates")
    DBI::dbExecute(conn, "DELETE FROM predictions")
    DBI::dbExecute(conn, "DELETE FROM customer_data")
  }

  if (get_customer_count(conn) == 0) {
    log_event("Loading customers", "Database is empty, loading initial data")
    customers <- load_initial_customer_data(cfg)
    upsert_customer_data(conn, customers)
  }

  customers <- get_customer_data(conn)
  training_data <- prepare_model_data(customers)

  if (isTRUE(force_reload) || !models_exist(cfg$paths$model_dir)) {
    log_event("Model training", "Training new models")
    split_data <- split_train_test(
      training_data,
      test_size = cfg$training$test_size,
      seed = cfg$project$seed
    )

    models <- train_churn_models(split_data$train, cfg)
    save_models(models, cfg$paths$model_dir)
    
    tryCatch(
      {
        log_event("Model evaluation", "Generating model report")
        save_model_report(models, split_data$test, cfg)
      },
      error = function(e) {
        log_event("Model report failed", e$message, is_error = TRUE)
      }
    )
  }

  models <- load_models(cfg$paths$model_dir)

  log_event("Pipeline execution", sprintf("Scoring %d customers", nrow(customers)))
  run_prediction_pipeline(conn, customers, models, cfg)

  log_event("Bootstrap complete", "System ready")
  invisible(TRUE)
}
