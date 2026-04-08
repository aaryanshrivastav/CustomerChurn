suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(ggplot2)
  library(scales)
})

source("R/config.R")
source("R/db.R")

cfg <- load_config("config/config.yml")
conn <- get_db_connection(cfg)
on.exit(DBI::dbDisconnect(conn), add = TRUE)

dir.create("reports/visualizations", recursive = TRUE, showWarnings = FALSE)

latest <- DBI::dbGetQuery(conn, "
  SELECT c.customer_id,
         c.contract_type,
         c.age,
         c.tenure_months,
         c.monthly_charges,
         c.support_tickets,
         c.last_login_days,
         c.usage_score,
         c.satisfaction_score,
         c.is_active,
         p.churn_probability,
         d.recommended_action,
         d.expected_benefit,
         d.expected_loss
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
    SELECT d1.* FROM intervention_decisions d1
    INNER JOIN (
      SELECT customer_id, MAX(decision_id) AS max_id
      FROM intervention_decisions
      GROUP BY customer_id
    ) d2 ON d1.customer_id = d2.customer_id AND d1.decision_id = d2.max_id
  ) d ON c.customer_id = d.customer_id
")

latest <- latest[!is.na(latest$churn_probability), , drop = FALSE]

if (nrow(latest) == 0) {
  stop("No scored rows found. Run scripts/train_and_bootstrap.R first.")
}

latest$contract_type <- factor(
  latest$contract_type,
  levels = c("month_to_month", "annual", "biannual"),
  labels = c("Month-to-month", "Annual", "Biannual")
)

latest$recommended_action <- factor(
  latest$recommended_action,
  levels = c("no_action", "retention_call", "discount"),
  labels = c("No Action", "Retention Call", "Discount")
)

# 1) Histogram + density of churn probabilities
p1 <- ggplot(latest, aes(x = churn_probability)) +
  geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "#8ecae6", color = "#1d3557", alpha = 0.8) +
  geom_density(color = "#d62828", linewidth = 1.1, adjust = 1.1) +
  scale_x_continuous(labels = percent_format(accuracy = 1)) +
  labs(
    title = "Churn Probability Distribution",
    x = "Churn Probability",
    y = "Density"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = "reports/visualizations/churn_probability_hist_density.png",
  plot = p1,
  width = 9,
  height = 5,
  dpi = 160
)

# 2) Box plots by contract type vs churn probability
p2 <- ggplot(latest, aes(x = contract_type, y = churn_probability, fill = contract_type)) +
  geom_boxplot(alpha = 0.8, outlier.alpha = 0.4) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_fill_manual(values = c("Month-to-month" = "#f4a261", "Annual" = "#2a9d8f", "Biannual" = "#457b9d")) +
  labs(
    title = "Churn Probability by Contract Type",
    x = "Contract Type",
    y = "Churn Probability"
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "none")

ggsave(
  filename = "reports/visualizations/boxplot_contract_vs_churn_probability.png",
  plot = p2,
  width = 8,
  height = 5,
  dpi = 160
)

# 3) Correlation heatmap for numeric engineered features
numeric_features <- latest[, c(
  "age", "tenure_months", "monthly_charges", "support_tickets",
  "last_login_days", "usage_score", "satisfaction_score", "is_active"
)]

cor_mat <- stats::cor(numeric_features, use = "pairwise.complete.obs")
cor_df <- as.data.frame(as.table(cor_mat), stringsAsFactors = FALSE)
names(cor_df) <- c("Feature_X", "Feature_Y", "Correlation")

p3 <- ggplot(cor_df, aes(x = Feature_X, y = Feature_Y, fill = Correlation)) +
  geom_tile(color = "white", linewidth = 0.2) +
  geom_text(aes(label = sprintf("%.2f", Correlation)), size = 3) +
  scale_fill_gradient2(low = "#4575b4", mid = "#f7f7f7", high = "#d73027", midpoint = 0, limits = c(-1, 1)) +
  labs(
    title = "Correlation Heatmap: Numeric Engineered Features",
    x = "",
    y = "",
    fill = "Corr"
  ) +
  theme_minimal(base_size = 11) +
  theme(axis.text.x = element_text(angle = 30, hjust = 1))

ggsave(
  filename = "reports/visualizations/correlation_heatmap_engineered_features.png",
  plot = p3,
  width = 9,
  height = 7,
  dpi = 170
)

# 4) Action-mix bar chart with expected benefit overlay
agg <- aggregate(
  cbind(customer_id, expected_benefit) ~ recommended_action,
  data = latest,
  FUN = function(x) c(sum = sum(x, na.rm = TRUE), n = length(x))
)

action_summary <- data.frame(
  recommended_action = agg$recommended_action,
  customers = agg$customer_id[, "n"],
  total_expected_benefit = agg$expected_benefit[, "sum"],
  stringsAsFactors = FALSE
)

max_customers <- max(action_summary$customers, na.rm = TRUE)
max_benefit <- max(action_summary$total_expected_benefit, na.rm = TRUE)
scale_factor <- ifelse(max_benefit > 0, max_customers / max_benefit, 1)

p4 <- ggplot(action_summary, aes(x = recommended_action, y = customers, fill = recommended_action)) +
  geom_col(width = 0.65, alpha = 0.85) +
  geom_line(
    aes(y = total_expected_benefit * scale_factor, group = 1, color = "Expected Benefit"),
    linewidth = 1.1
  ) +
  geom_point(
    aes(y = total_expected_benefit * scale_factor, color = "Expected Benefit"),
    size = 2.5
  ) +
  scale_fill_manual(values = c("No Action" = "#90a4ae", "Retention Call" = "#ffb703", "Discount" = "#2a9d8f")) +
  scale_color_manual(values = c("Expected Benefit" = "#e63946")) +
  scale_y_continuous(
    name = "Customer Count",
    sec.axis = sec_axis(~ . / scale_factor, name = "Total Expected Benefit", labels = dollar_format())
  ) +
  labs(
    title = "Action Mix with Expected Benefit Overlay",
    x = "Recommended Action",
    fill = "Action",
    color = "Overlay"
  ) +
  theme_minimal(base_size = 12)

ggsave(
  filename = "reports/visualizations/action_mix_with_expected_benefit_overlay.png",
  plot = p4,
  width = 9,
  height = 5.5,
  dpi = 170
)

cat("Generated visualizations:\n")
cat("- reports/visualizations/churn_probability_hist_density.png\n")
cat("- reports/visualizations/boxplot_contract_vs_churn_probability.png\n")
cat("- reports/visualizations/correlation_heatmap_engineered_features.png\n")
cat("- reports/visualizations/action_mix_with_expected_benefit_overlay.png\n")