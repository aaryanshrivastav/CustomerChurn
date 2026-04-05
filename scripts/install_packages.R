required_packages <- c(
  "RSQLite", "DBI", "yaml", "dplyr", "tibble", "purrr", "tidyr",
  "lubridate", "randomForest", "survival", "shiny", "ggplot2", "scales",
  "jsonlite", "pROC"
)

installed <- rownames(installed.packages())
missing <- setdiff(required_packages, installed)

if (length(missing) > 0) {
  message("Installing missing packages: ", paste(missing, collapse = ", "))
  install.packages(missing, repos = "https://cloud.r-project.org")
} else {
  message("All required packages are already installed.")
}
