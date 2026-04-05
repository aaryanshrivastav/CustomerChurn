load_config <- function(config_path = "config/config.yml") {
  if (!file.exists(config_path)) {
    stop("Config file not found at: ", config_path)
  }

  cfg <- yaml::read_yaml(config_path)

  if (is.null(cfg$paths$db_path) || is.null(cfg$paths$model_dir)) {
    stop("Invalid configuration: missing paths.db_path or paths.model_dir")
  }

  db_dir <- dirname(cfg$paths$db_path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)
  }

  if (!dir.exists(cfg$paths$model_dir)) {
    dir.create(cfg$paths$model_dir, recursive = TRUE, showWarnings = FALSE)
  }

  cfg
}
