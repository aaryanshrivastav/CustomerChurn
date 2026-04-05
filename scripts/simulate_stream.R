suppressPackageStartupMessages({
  library(yaml)
  library(DBI)
  library(RSQLite)
  library(randomForest)
  library(survival)
})

source("R/config.R")
source("R/db.R")
source("R/error_handler.R")
source("R/validation.R")
source("R/logging.R")
source("R/data_generation.R")
source("R/data_ingestion.R")
source("R/modeling.R")
source("R/model_evaluation.R")
source("R/decision_engine.R")
source("R/pipeline.R")
source("R/simulation.R")

cfg <- load_config("config/config.yml")
conn <- get_db_connection(cfg)
on.exit(DBI::dbDisconnect(conn), add = TRUE)

if (!models_exist(cfg$paths$model_dir)) {
  stop("Models not found. Run scripts/train_and_bootstrap.R first.")
}

models <- load_models(cfg$paths$model_dir)

args <- commandArgs(trailingOnly = TRUE)
iterations <- if (length(args) >= 1) as.integer(args[[1]]) else 10
batch_size <- if (length(args) >= 2) as.integer(args[[2]]) else cfg$simulation$batch_size
sleep_seconds <- if (length(args) >= 3) as.numeric(args[[3]]) else 1

for (i in seq_len(iterations)) {
  scored <- simulate_realtime_updates(conn, models, cfg, batch_size = batch_size)
  avg_churn <- if (nrow(scored) > 0) round(mean(scored$churn_probability), 4) else NA_real_
  total_loss <- if (nrow(scored) > 0) round(sum(scored$expected_loss), 2) else 0

  cat(sprintf(
    "Iteration %d | Updated customers: %d | Avg churn: %s | Expected loss: %.2f\n",
    i,
    nrow(scored),
    as.character(avg_churn),
    total_loss
  ))

  Sys.sleep(sleep_seconds)
}
