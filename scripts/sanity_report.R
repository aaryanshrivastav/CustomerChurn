suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

source("R/config.R")
source("R/db.R")

cfg <- load_config("config/config.yml")
conn <- get_db_connection(cfg)
on.exit(DBI::dbDisconnect(conn), add = TRUE)

view <- get_latest_customer_view(conn)

cat("Rows:\n")
print(nrow(view))

cat("\nChurn Probability Summary:\n")
print(summary(view$churn_probability))

cat("\nExpected Time-to-Churn Summary:\n")
print(summary(view$expected_time_to_churn))

cat("\nPredicted CLV Summary:\n")
print(summary(view$predicted_clv))

cat("\nExpected Loss Summary:\n")
print(summary(view$expected_loss))

cat("\nAction Distribution:\n")
print(table(view$recommended_action, useNA = "ifany"))

cat("\nExpected Benefit Summary:\n")
print(summary(view$expected_benefit))
