validate_customer_input <- function(customer) {
  errors <- c()

  if (is.na(customer$age) || customer$age < 18 || customer$age > 100) {
    errors <- c(errors, "Age must be between 18-100")
  }
  if (is.na(customer$tenure_months) || customer$tenure_months < 0 || customer$tenure_months > 240) {
    errors <- c(errors, "Tenure must be 0-240 months")
  }
  if (is.na(customer$monthly_charges) || customer$monthly_charges < 1 || customer$monthly_charges > 500) {
    errors <- c(errors, "Monthly charges must be $1-$500")
  }
  if (is.na(customer$support_tickets) || customer$support_tickets < 0 || customer$support_tickets > 50) {
    errors <- c(errors, "Support tickets must be 0-50")
  }
  if (is.na(customer$last_login_days) || customer$last_login_days < 0 || customer$last_login_days > 120) {
    errors <- c(errors, "Days since login must be 0-120")
  }
  if (is.na(customer$usage_score) || customer$usage_score < 0 || customer$usage_score > 100) {
    errors <- c(errors, "Usage score must be 0-100")
  }
  if (is.na(customer$satisfaction_score) || customer$satisfaction_score < 0 || customer$satisfaction_score > 100) {
    errors <- c(errors, "Satisfaction score must be 0-100")
  }

  if (length(errors) > 0) {
    stop(paste(errors, collapse = "; "))
  }

  invisible(TRUE)
}

validate_predictions <- function(pred_df) {
  warnings_list <- c()

  if (any(is.na(pred_df$churn_probability))) {
    warnings_list <- c(warnings_list, "NA values in churn_probability detected")
  }
  if (any(pred_df$churn_probability < 0 | pred_df$churn_probability > 1)) {
    warnings_list <- c(warnings_list, "Out-of-range churn probabilities detected")
  }
  if (any(pred_df$predicted_clv < 0, na.rm = TRUE)) {
    warnings_list <- c(warnings_list, "Negative CLV predicted")
  }

  if (length(warnings_list) > 0) {
    warning(paste(warnings_list, collapse = "; "))
  }

  invisible(TRUE)
}

validate_dataframe_structure <- function(df, required_cols) {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }
  invisible(TRUE)
}
