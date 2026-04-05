generate_synthetic_customers <- function(n_customers, seed = 42) {
  set.seed(seed)

  customer_id <- seq_len(n_customers)
  age <- sample(18:80, n_customers, replace = TRUE)
  tenure_months <- sample(1:120, n_customers, replace = TRUE)
  monthly_charges <- round(runif(n_customers, 25, 220), 2)
  support_tickets <- rpois(n_customers, lambda = 1.7)
  last_login_days <- sample(0:45, n_customers, replace = TRUE)
  contract_type <- sample(
    c("month_to_month", "annual", "biannual"),
    n_customers,
    replace = TRUE,
    prob = c(0.55, 0.30, 0.15)
  )
  payment_method <- sample(
    c("credit_card", "bank_transfer", "wallet", "cash"),
    n_customers,
    replace = TRUE,
    prob = c(0.50, 0.20, 0.20, 0.10)
  )
  usage_score <- pmax(0, pmin(100, rnorm(n_customers, mean = 62, sd = 18)))
  satisfaction_score <- pmax(0, pmin(100, rnorm(n_customers, mean = 66, sd = 20)))
  is_active <- as.integer(last_login_days <= 15)

  linear_term <-
    -2.1 +
    0.018 * support_tickets +
    0.028 * last_login_days -
    0.015 * tenure_months +
    0.007 * monthly_charges -
    0.030 * usage_score -
    0.030 * satisfaction_score +
    0.25 * as.integer(contract_type == "month_to_month") -
    0.35 * as.integer(contract_type == "biannual")

  churn_probability_true <- plogis(linear_term)
  churned <- rbinom(n_customers, size = 1, prob = churn_probability_true)

  base_rate <- 1 / 40
  hazard <- base_rate * exp(scale(linear_term))
  event_time <- rexp(n_customers, rate = pmax(0.005, hazard))
  censor_time <- runif(n_customers, min = 6, max = 72)

  duration <- pmin(event_time, censor_time)
  event <- as.integer(event_time <= censor_time)

  monthly_margin <- round(monthly_charges * runif(n_customers, 0.30, 0.65), 2)
  remaining_months <- pmax(1, round((1 - churn_probability_true) * runif(n_customers, 12, 48)))
  true_clv <- round(monthly_margin * remaining_months, 2)

  data.frame(
    customer_id = customer_id,
    age = age,
    tenure_months = tenure_months,
    monthly_charges = monthly_charges,
    support_tickets = support_tickets,
    last_login_days = last_login_days,
    contract_type = contract_type,
    payment_method = payment_method,
    usage_score = round(usage_score, 2),
    satisfaction_score = round(satisfaction_score, 2),
    is_active = is_active,
    churned = churned,
    duration = round(duration, 3),
    event = event,
    monthly_margin = monthly_margin,
    true_clv = true_clv,
    updated_at = as.character(Sys.time()),
    stringsAsFactors = FALSE
  )
}

generate_customer_updates <- function(customers_df, batch_size, cfg) {
  if (nrow(customers_df) == 0) {
    return(customers_df)
  }

  n <- min(batch_size, nrow(customers_df))
  idx <- sample(seq_len(nrow(customers_df)), size = n, replace = FALSE)
  updates <- customers_df[idx, , drop = FALSE]

  updates$support_tickets <- pmax(
    0,
    updates$support_tickets + sample(0:cfg$simulation$max_support_tickets_increment, n, replace = TRUE)
  )

  updates$last_login_days <- pmax(
    0,
    updates$last_login_days + sample(0:cfg$simulation$max_last_login_days_increment, n, replace = TRUE)
  )

  updates$usage_score <- pmax(
    0,
    pmin(100, updates$usage_score + rnorm(n, mean = 0, sd = cfg$simulation$usage_noise_sd))
  )

  updates$satisfaction_score <- pmax(
    0,
    pmin(100, updates$satisfaction_score + rnorm(n, mean = -1, sd = cfg$simulation$satisfaction_noise_sd))
  )

  updates$is_active <- as.integer(updates$last_login_days <= 15)
  updates$updated_at <- as.character(Sys.time())

  updates
}
