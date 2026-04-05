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
source("R/bootstrap.R")

cfg <- load_config("config/config.yml")

if (!file.exists(cfg$paths$raw_csv_path)) {
  stop("CSV file not found at configured path: ", cfg$paths$raw_csv_path)
}

bootstrap_system(cfg, force_reload = TRUE)

cat("CSV reload completed. Database replaced from CSV source and models refreshed.\n")
