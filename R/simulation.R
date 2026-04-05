simulate_realtime_updates <- function(conn, models, cfg, batch_size = cfg$simulation$batch_size) {
  customers <- get_customer_data(conn)
  updates <- generate_customer_updates(customers, batch_size = batch_size, cfg = cfg)

  if (nrow(updates) == 0) {
    return(updates)
  }

  upsert_customer_data(conn, updates)
  run_prediction_pipeline(conn, updates, models, cfg)
}
