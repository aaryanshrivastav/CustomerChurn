clean_total_charges <- function(x, monthly_charges, tenure) {
  x_chr <- trimws(as.character(x))
  x_num <- suppressWarnings(as.numeric(x_chr))

  missing_idx <- is.na(x_num)
  if (any(missing_idx)) {
    # Backfill missing total charges with an approximate tenure * monthly pattern.
    x_num[missing_idx] <- monthly_charges[missing_idx] * pmax(tenure[missing_idx], 1)
  }

  x_num
}

standardize_payment_method <- function(x) {
  x_lower <- tolower(x)
  out <- ifelse(grepl("credit card", x_lower), "credit_card", "wallet")
  out <- ifelse(grepl("bank transfer", x_lower), "bank_transfer", out)
  out <- ifelse(grepl("mailed check|electronic check", x_lower), "cash", out)
  out
}

standardize_contract <- function(x) {
  out <- ifelse(x == "Month-to-month", "month_to_month", "month_to_month")
  out <- ifelse(x == "One year", "annual", out)
  out <- ifelse(x == "Two year", "biannual", out)
  out
}

validate_telco_schema <- function(raw) {
  required <- c(
    "customerID", "SeniorCitizen", "Partner", "Dependents", "tenure",
    "InternetService", "OnlineSecurity", "OnlineBackup", "DeviceProtection",
    "TechSupport", "StreamingTV", "StreamingMovies", "Contract",
    "PaperlessBilling", "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn"
  )

  missing <- setdiff(required, names(raw))
  if (length(missing) > 0) {
    stop("CSV is missing required columns: ", paste(missing, collapse = ", "))
  }
}

estimate_remaining_months <- function(contract, tenure) {
  remaining <- ifelse(
    contract == "Two year",
    pmax(1, 24 - (tenure %% 24)) + 8,
    ifelse(contract == "One year", pmax(1, 12 - (tenure %% 12)) + 5, pmax(3, round(6 + 0.08 * tenure)))
  )

  pmax(1, pmin(60, remaining))
}

load_customers_from_csv <- function(csv_path, seed = 42) {
  if (!file.exists(csv_path)) {
    stop("CSV file not found at: ", csv_path)
  }

  set.seed(seed)

  raw <- read.csv(csv_path, stringsAsFactors = FALSE)
  if (nrow(raw) == 0) {
    stop("CSV has no rows: ", csv_path)
  }
  validate_telco_schema(raw)

  monthly_charges <- as.numeric(raw$MonthlyCharges)
  tenure <- as.integer(raw$tenure)
  clean_total_charges(raw$TotalCharges, monthly_charges, tenure)

  churned <- as.integer(raw$Churn == "Yes")
  senior <- as.integer(raw$SeniorCitizen)

  age <- ifelse(senior == 1, 65 + pmin(15, tenure %/% 12), 27 + pmin(32, tenure %/% 2))

  enabled_services <-
    as.integer(raw$OnlineSecurity == "Yes") +
    as.integer(raw$OnlineBackup == "Yes") +
    as.integer(raw$DeviceProtection == "Yes") +
    as.integer(raw$TechSupport == "Yes") +
    as.integer(raw$StreamingTV == "Yes") +
    as.integer(raw$StreamingMovies == "Yes")

  support_tickets <- pmax(0, pmin(
    12,
    1 +
      as.integer(raw$TechSupport == "No") +
      as.integer(raw$OnlineSecurity == "No") +
      as.integer(raw$InternetService == "Fiber optic") +
      as.integer(raw$PaperlessBilling == "Yes" & grepl("electronic check", tolower(raw$PaymentMethod))) +
      round((72 - pmin(72, tenure)) / 24)
  ))

  household_stability <- as.integer(raw$Partner == "Yes") + as.integer(raw$Dependents == "Yes")

  last_login_days <- pmax(0, pmin(
    45,
    round(
      14 - 0.10 * pmin(tenure, 72) -
        1.2 * enabled_services -
        1.5 * household_stability +
        ifelse(raw$Contract == "Month-to-month", 6, 0) +
        stats::rnorm(nrow(raw), 0, 2)
    )
  ))

  usage_score <- pmax(
    0,
    pmin(
      100,
      round(
        30 +
          7 * enabled_services +
          0.25 * pmin(monthly_charges, 120) +
          0.22 * pmin(tenure, 72) -
          0.6 * last_login_days +
          ifelse(raw$InternetService == "No", -15, 0) +
          stats::rnorm(nrow(raw), 0, 5),
        2
      )
    )
  )

  satisfaction_score <- pmax(
    0,
    pmin(
      100,
      round(
        52 +
          6 * as.integer(raw$TechSupport == "Yes") +
          4 * as.integer(raw$OnlineSecurity == "Yes") +
          3 * as.integer(raw$OnlineBackup == "Yes") -
          4 * as.integer(raw$Contract != "Month-to-month") -
          4 * as.integer(grepl("electronic check", tolower(raw$PaymentMethod))) +
          stats::rnorm(nrow(raw), 0, 6),
        2
      )
    )
  )

  contract_type <- standardize_contract(raw$Contract)
  payment_method <- standardize_payment_method(raw$PaymentMethod)

  is_active <- as.integer(last_login_days <= 15)
  duration <- pmax(1, as.numeric(tenure))
  event <- churned

  monthly_margin <- round(monthly_charges * 0.42, 2)
  estimated_remaining <- estimate_remaining_months(raw$Contract, tenure)
  true_clv <- round(monthly_margin * estimated_remaining, 2)

  data.frame(
    customer_id = seq_len(nrow(raw)),
    age = as.integer(age),
    tenure_months = as.integer(tenure),
    monthly_charges = as.numeric(monthly_charges),
    support_tickets = as.integer(support_tickets),
    last_login_days = as.integer(last_login_days),
    contract_type = contract_type,
    payment_method = payment_method,
    usage_score = as.numeric(usage_score),
    satisfaction_score = as.numeric(satisfaction_score),
    is_active = as.integer(is_active),
    churned = as.integer(churned),
    duration = as.numeric(duration),
    event = as.integer(event),
    monthly_margin = as.numeric(monthly_margin),
    true_clv = as.numeric(true_clv),
    updated_at = as.character(Sys.time()),
    stringsAsFactors = FALSE
  )
}

load_initial_customer_data <- function(cfg) {
  use_csv <- isTRUE(cfg$data_source$prefer_csv)
  csv_path <- cfg$paths$raw_csv_path

  if (use_csv && file.exists(csv_path)) {
    log_event("Data loading", paste0("Loading CSV: ", csv_path))
    result <- safe_csv_load(
      csv_path,
      fallback_to_synthetic = isTRUE(cfg$data_source$fallback_to_synthetic),
      seed = cfg$project$seed
    )
    return(result)
  }

  if (!isTRUE(cfg$data_source$fallback_to_synthetic)) {
    stop("CSV data source unavailable and fallback_to_synthetic is disabled.")
  }

  log_event("Data loading", "CSV unavailable, using synthetic data")
  generate_synthetic_customers(
    n_customers = cfg$training$n_customers,
    seed = cfg$project$seed
  )
}
