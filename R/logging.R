setup_logging <- function() {
  dir.create("logs", showWarnings = FALSE)
  
  cat(
    paste0("=== Log Session Started: ", Sys.time(), " ===\n"),
    file = "logs/system.log",
    append = TRUE
  )
}

log_event <- function(event_type, details, is_error = FALSE) {
  level <- if (is_error) "ERROR" else "INFO"
  
  message_text <- sprintf(
    "[%s] %s: %s\n",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    level,
    event_type
  )
  
  if (!is.null(details)) {
    message_text <- paste0(message_text, "  Details: ", details, "\n")
  }
  
  cat(message_text, file = "logs/system.log", append = TRUE)
  
  if (is_error) {
    warning(message_text)
  }
  
  invisible(NULL)
}

log_prediction <- function(conn, customer_id, prediction_result) {
  tryCatch(
    {
      DBI::dbExecute(
        conn,
        "INSERT INTO audit_log (event_type, customer_id, details, created_at)
         VALUES (?, ?, ?, ?)",
        params = list(
          "prediction_generated",
          customer_id,
          jsonlite::toJSON(prediction_result, auto_unbox = TRUE),
          as.character(Sys.time())
        )
      )
    },
    error = function(e) {
      log_event("Audit log write failed", e$message, is_error = TRUE)
    }
  )
}
