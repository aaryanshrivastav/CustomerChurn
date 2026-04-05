compute_action_outcome <- function(churn_probability, clv, action_cfg) {
  effectiveness <- action_cfg$effectiveness
  if (is.null(effectiveness)) {
    effectiveness <- action_cfg$churn_reduction
  }
  if (is.null(effectiveness)) {
    effectiveness <- 0
  }

  adjusted_churn <- pmax(0, churn_probability * (1 - effectiveness))
  adjusted_loss <- (adjusted_churn * clv) + action_cfg$cost
  adjusted_loss
}

recommend_interventions <- function(prediction_df, cfg) {
  actions <- cfg$decision_engine$actions

  expected_loss <- prediction_df$churn_probability * prediction_df$predicted_clv

  loss_discount <- compute_action_outcome(
    prediction_df$churn_probability,
    prediction_df$predicted_clv,
    actions$discount
  )

  loss_retention_call <- compute_action_outcome(
    prediction_df$churn_probability,
    prediction_df$predicted_clv,
    actions$retention_call
  )

  loss_no_action <- compute_action_outcome(
    prediction_df$churn_probability,
    prediction_df$predicted_clv,
    actions$no_action
  )

  action_matrix <- cbind(
    discount = loss_discount,
    retention_call = loss_retention_call,
    no_action = loss_no_action
  )

  min_idx <- max.col(-action_matrix)
  action_names <- colnames(action_matrix)
  recommended_action <- action_names[min_idx]

  minimum_loss <- action_matrix[cbind(seq_len(nrow(action_matrix)), min_idx)]
  expected_benefit <- expected_loss - minimum_loss

  recommended_action[expected_benefit <= 0] <- "no_action"

  data.frame(
    customer_id = prediction_df$customer_id,
    expected_loss = as.numeric(expected_loss),
    recommended_action = recommended_action,
    expected_benefit = as.numeric(pmax(0, expected_benefit)),
    loss_discount = as.numeric(loss_discount),
    loss_retention_call = as.numeric(loss_retention_call),
    loss_no_action = as.numeric(loss_no_action),
    stringsAsFactors = FALSE
  )
}
