get_feature_columns <- function() {
  c(
    "age", "tenure_months", "monthly_charges", "support_tickets",
    "last_login_days", "contract_type", "payment_method",
    "usage_score", "satisfaction_score", "is_active"
  )
}

prepare_model_data <- function(df) {
  df <- df[, c(get_feature_columns(), "churned", "duration", "event", "true_clv")]
  df$contract_type <- as.factor(df$contract_type)
  df$payment_method <- as.factor(df$payment_method)
  df$churned <- as.integer(df$churned)
  df$event <- as.integer(df$event)
  df
}

split_train_test <- function(df, test_size = 0.2, seed = 42) {
  set.seed(seed)
  n <- nrow(df)
  test_n <- floor(n * test_size)
  test_idx <- sample(seq_len(n), size = test_n)

  list(
    train = df[-test_idx, , drop = FALSE],
    test = df[test_idx, , drop = FALSE]
  )
}

train_churn_models <- function(train_df, cfg) {
  formula_churn <- stats::as.formula(
    churned ~ age + tenure_months + monthly_charges + support_tickets +
      last_login_days + contract_type + payment_method + usage_score +
      satisfaction_score + is_active
  )

  logistic_model <- stats::glm(
    formula_churn,
    data = train_df,
    family = stats::binomial(link = "logit")
  )

  rf_model <- randomForest::randomForest(
    x = train_df[, get_feature_columns()],
    y = as.factor(train_df$churned),
    ntree = cfg$training$random_forest_trees
  )

  formula_survival <- stats::as.formula(
    survival::Surv(duration, event) ~ age + tenure_months + monthly_charges +
      support_tickets + last_login_days + contract_type + payment_method +
      usage_score + satisfaction_score + is_active
  )

  cox_model <- survival::coxph(formula_survival, data = train_df)

  baseline_hazard <- survival::basehaz(cox_model, centered = FALSE)
  baseline_hazard <- baseline_hazard[order(baseline_hazard$time), c("time", "hazard")]
  baseline_hazard <- baseline_hazard[!duplicated(baseline_hazard$time), , drop = FALSE]

  factor_levels <- list(
    contract_type = levels(train_df$contract_type),
    payment_method = levels(train_df$payment_method)
  )

  list(
    logistic_model = logistic_model,
    rf_model = rf_model,
    cox_model = cox_model,
    baseline_hazard = baseline_hazard,
    factor_levels = factor_levels,
    training_event_rate = mean(train_df$event, na.rm = TRUE)
  )
}

save_models <- function(models, model_dir) {
  saveRDS(models$logistic_model, file.path(model_dir, "logistic_model.rds"))
  saveRDS(models$rf_model, file.path(model_dir, "rf_model.rds"))
  saveRDS(models$cox_model, file.path(model_dir, "cox_model.rds"))
  saveRDS(
    list(
      factor_levels = models$factor_levels,
      baseline_hazard = models$baseline_hazard,
      training_event_rate = models$training_event_rate
    ),
    file.path(model_dir, "model_metadata.rds")
  )
}

load_models <- function(model_dir) {
  metadata <- readRDS(file.path(model_dir, "model_metadata.rds"))
  cox_model <- readRDS(file.path(model_dir, "cox_model.rds"))

  baseline_hazard <- metadata$baseline_hazard
  if (is.null(baseline_hazard)) {
    baseline_hazard <- survival::basehaz(cox_model, centered = FALSE)
    baseline_hazard <- baseline_hazard[order(baseline_hazard$time), c("time", "hazard")]
    baseline_hazard <- baseline_hazard[!duplicated(baseline_hazard$time), , drop = FALSE]
  }

  training_event_rate <- metadata$training_event_rate
  if (is.null(training_event_rate) || !is.finite(training_event_rate)) {
    training_event_rate <- 0.25
  }

  list(
    logistic_model = readRDS(file.path(model_dir, "logistic_model.rds")),
    rf_model = readRDS(file.path(model_dir, "rf_model.rds")),
    cox_model = cox_model,
    factor_levels = metadata$factor_levels,
    baseline_hazard = baseline_hazard,
    training_event_rate = training_event_rate
  )
}

models_exist <- function(model_dir) {
  required <- c(
    "logistic_model.rds", "rf_model.rds", "cox_model.rds", "model_metadata.rds"
  )
  all(file.exists(file.path(model_dir, required)))
}

coerce_prediction_features <- function(new_df, factor_levels) {
  pred_df <- new_df
  pred_df$contract_type <- factor(pred_df$contract_type, levels = factor_levels$contract_type)
  pred_df$payment_method <- factor(pred_df$payment_method, levels = factor_levels$payment_method)

  pred_df
}

predict_customer_batch <- function(models, new_df, cfg) {
  pred_df <- coerce_prediction_features(new_df, models$factor_levels)

  p_logit <- stats::predict(models$logistic_model, newdata = pred_df, type = "response")

  p_rf <- stats::predict(models$rf_model, newdata = pred_df[, get_feature_columns()], type = "prob")[, "1"]

  p_churn <- (cfg$training$logistic_weight * p_logit) + (cfg$training$rf_weight * p_rf)

  linear_predictor <- stats::predict(models$cox_model, newdata = pred_df, type = "lp")
  # Clip LP before exponentiation to keep survival risk numerically stable.
  relative_risk <- exp(pmin(pmax(linear_predictor, -8), 8))

  surv_summary <- summary(survival::survfit(models$cox_model, newdata = pred_df))
  surv_table <- surv_summary$table

  if (is.null(dim(surv_table))) {
    median_total_time <- as.numeric(surv_table["median"])
    rmean_total_time <- as.numeric(surv_table["rmean"])
  } else {
    median_total_time <- as.numeric(surv_table[, "median"])
    rmean_total_time <- as.numeric(surv_table[, "rmean"])
  }

  # Fall back to restricted mean survival time when median is unavailable.
  median_total_time[is.na(median_total_time) | !is.finite(median_total_time)] <-
    rmean_total_time[is.na(median_total_time) | !is.finite(median_total_time)]

  # Final safety net if both summary statistics are unavailable.
  median_total_time[is.na(median_total_time) | !is.finite(median_total_time)] <-
    pred_df$tenure_months[is.na(median_total_time) | !is.finite(median_total_time)] + cfg$clv$min_remaining_months

  expected_time <- pmax(
    cfg$clv$min_remaining_months,
    pmin(cfg$clv$max_remaining_months, median_total_time - pred_df$tenure_months)
  )

  monthly_margin <- pred_df$monthly_charges * cfg$clv$gross_margin_rate
  monthly_discount <- cfg$clv$monthly_discount_rate

  discount_factor <- if (monthly_discount > 0) {
    (1 - (1 + monthly_discount)^(-expected_time)) / monthly_discount
  } else {
    expected_time
  }

  clv_pred <- pmax(0, monthly_margin * discount_factor)

  data.frame(
    customer_id = pred_df$customer_id,
    churn_prob_logit = as.numeric(p_logit),
    churn_prob_rf = as.numeric(p_rf),
    churn_probability = as.numeric(p_churn),
    survival_risk = as.numeric(relative_risk),
    expected_time_to_churn = as.numeric(expected_time),
    predicted_clv = as.numeric(clv_pred),
    stringsAsFactors = FALSE
  )
}
