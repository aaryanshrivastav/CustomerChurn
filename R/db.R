get_db_connection <- function(cfg) {
  DBI::dbConnect(RSQLite::SQLite(), cfg$paths$db_path)
}

initialize_database <- function(conn) {
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS customer_data (
      customer_id INTEGER PRIMARY KEY,
      age INTEGER,
      tenure_months INTEGER,
      monthly_charges REAL,
      support_tickets INTEGER,
      last_login_days INTEGER,
      contract_type TEXT,
      payment_method TEXT,
      usage_score REAL,
      satisfaction_score REAL,
      is_active INTEGER,
      churned INTEGER,
      duration REAL,
      event INTEGER,
      monthly_margin REAL,
      true_clv REAL,
      updated_at TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS predictions (
      prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER,
      churn_prob_logit REAL,
      churn_prob_rf REAL,
      churn_probability REAL,
      model_run_ts TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS survival_estimates (
      estimate_id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER,
      survival_risk REAL,
      expected_time_to_churn REAL,
      model_run_ts TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS clv_values (
      clv_id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER,
      predicted_clv REAL,
      model_run_ts TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS intervention_decisions (
      decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id INTEGER,
      expected_loss REAL,
      recommended_action TEXT,
      expected_benefit REAL,
      loss_discount REAL,
      loss_retention_call REAL,
      loss_no_action REAL,
      model_run_ts TEXT
    )
  ")

  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS audit_log (
      log_id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_type TEXT,
      customer_id INTEGER,
      details TEXT,
      created_at TEXT
    )
  ")
}

upsert_customer_data <- function(conn, customer_df) {
  if (nrow(customer_df) == 0) {
    return(invisible(NULL))
  }

  sql <- "
    INSERT INTO customer_data (
      customer_id, age, tenure_months, monthly_charges, support_tickets,
      last_login_days, contract_type, payment_method, usage_score,
      satisfaction_score, is_active, churned, duration, event,
      monthly_margin, true_clv, updated_at
    ) VALUES (
      :customer_id, :age, :tenure_months, :monthly_charges, :support_tickets,
      :last_login_days, :contract_type, :payment_method, :usage_score,
      :satisfaction_score, :is_active, :churned, :duration, :event,
      :monthly_margin, :true_clv, :updated_at
    )
    ON CONFLICT(customer_id) DO UPDATE SET
      age = excluded.age,
      tenure_months = excluded.tenure_months,
      monthly_charges = excluded.monthly_charges,
      support_tickets = excluded.support_tickets,
      last_login_days = excluded.last_login_days,
      contract_type = excluded.contract_type,
      payment_method = excluded.payment_method,
      usage_score = excluded.usage_score,
      satisfaction_score = excluded.satisfaction_score,
      is_active = excluded.is_active,
      churned = excluded.churned,
      duration = excluded.duration,
      event = excluded.event,
      monthly_margin = excluded.monthly_margin,
      true_clv = excluded.true_clv,
      updated_at = excluded.updated_at
  "

  rows <- split(customer_df, seq_len(nrow(customer_df)))
  for (row in rows) {
    DBI::dbExecute(conn, sql, params = as.list(row[1, ]))
  }

  invisible(NULL)
}

append_predictions <- function(conn, prediction_df) {
  DBI::dbWriteTable(conn, "predictions", prediction_df, append = TRUE, row.names = FALSE)
}

append_survival_estimates <- function(conn, survival_df) {
  DBI::dbWriteTable(conn, "survival_estimates", survival_df, append = TRUE, row.names = FALSE)
}

append_clv_values <- function(conn, clv_df) {
  DBI::dbWriteTable(conn, "clv_values", clv_df, append = TRUE, row.names = FALSE)
}

append_intervention_decisions <- function(conn, decision_df) {
  DBI::dbWriteTable(conn, "intervention_decisions", decision_df, append = TRUE, row.names = FALSE)
}

get_customer_data <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT * FROM customer_data")
}

get_customer_count <- function(conn) {
  DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM customer_data")$n[[1]]
}

get_recent_predictions <- function(conn, limit = 500) {
  DBI::dbGetQuery(
    conn,
    paste0(
      "SELECT * FROM predictions ORDER BY prediction_id DESC LIMIT ",
      as.integer(limit)
    )
  )
}

get_latest_customer_view <- function(conn) {
  DBI::dbGetQuery(conn, "
    SELECT c.customer_id,
           c.tenure_months,
           c.monthly_charges,
           c.support_tickets,
           c.last_login_days,
           c.usage_score,
           c.satisfaction_score,
           p.churn_probability,
           s.expected_time_to_churn,
           v.predicted_clv,
           d.expected_loss,
           d.recommended_action,
           d.expected_benefit,
           p.model_run_ts
    FROM customer_data c
    LEFT JOIN (
      SELECT p1.* FROM predictions p1
      INNER JOIN (
        SELECT customer_id, MAX(prediction_id) AS max_id
        FROM predictions
        GROUP BY customer_id
      ) p2 ON p1.customer_id = p2.customer_id AND p1.prediction_id = p2.max_id
    ) p ON c.customer_id = p.customer_id
    LEFT JOIN (
      SELECT s1.* FROM survival_estimates s1
      INNER JOIN (
        SELECT customer_id, MAX(estimate_id) AS max_id
        FROM survival_estimates
        GROUP BY customer_id
      ) s2 ON s1.customer_id = s2.customer_id AND s1.estimate_id = s2.max_id
    ) s ON c.customer_id = s.customer_id
    LEFT JOIN (
      SELECT v1.* FROM clv_values v1
      INNER JOIN (
        SELECT customer_id, MAX(clv_id) AS max_id
        FROM clv_values
        GROUP BY customer_id
      ) v2 ON v1.customer_id = v2.customer_id AND v1.clv_id = v2.max_id
    ) v ON c.customer_id = v.customer_id
    LEFT JOIN (
      SELECT d1.* FROM intervention_decisions d1
      INNER JOIN (
        SELECT customer_id, MAX(decision_id) AS max_id
        FROM intervention_decisions
        GROUP BY customer_id
      ) d2 ON d1.customer_id = d2.customer_id AND d1.decision_id = d2.max_id
    ) d ON c.customer_id = d.customer_id
  ")
}
