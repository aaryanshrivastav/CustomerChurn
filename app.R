suppressPackageStartupMessages({
  library(shiny)
  library(DBI)
  library(RSQLite)
  library(ggplot2)
  library(scales)
  library(randomForest)
  library(survival)
  library(yaml)
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
bootstrap_system(cfg)

conn <- get_db_connection(cfg)
models <- load_models(cfg$paths$model_dir)

onStop(function() {
  DBI::dbDisconnect(conn)
})

risk_band <- function(p, cfg) {
  ifelse(
    p >= cfg$ui$high_risk_cutoff,
    "High",
    ifelse(p >= cfg$ui$medium_risk_cutoff, "Medium", "Low")
  )
}

format_action_label <- function(x) {
  out <- gsub("_", " ", as.character(x), fixed = TRUE)
  out <- tools::toTitleCase(out)
  out[is.na(out) | out == ""] <- "N/A"
  out
}

build_kpi_card <- function(title, value, subtitle = NULL, description = NULL) {
  div(
    class = "kpi-card",
    div(class = "kpi-title", title),
    div(class = "kpi-value", value),
    if (!is.null(subtitle)) div(class = "kpi-subtitle", subtitle),
    if (!is.null(description)) div(class = "kpi-help", description)
  )
}

ui <- fluidPage(
  tags$head(
    tags$link(rel = "preconnect", href = "https://fonts.googleapis.com"),
    tags$link(rel = "preconnect", href = "https://fonts.gstatic.com", crossorigin = ""),
    tags$link(
      href = "https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Space+Grotesk:wght@500;700&display=swap",
      rel = "stylesheet"
    ),
    tags$style(HTML(" 
      body {
        font-family: 'Manrope', sans-serif;
        background: radial-gradient(circle at 12% 18%, #18233a 0%, #0f172b 42%, #0b1324 70%, #070d1a 100%);
        color: #d9e7ff;
        min-height: 100vh;
      }
      .app-shell {
        padding: 22px 16px 30px 16px;
      }
      .hero {
        border-radius: 22px;
        padding: 24px;
        color: #e6eeff;
        background: linear-gradient(140deg, rgba(28,40,66,0.9), rgba(17,26,44,0.88));
        border: 1px solid rgba(114,141,196,0.35);
        box-shadow: 0 14px 34px rgba(3,8,18,0.55);
        backdrop-filter: blur(16px);
        margin-bottom: 16px;
      }
      .hero h2 {
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 700;
        margin-top: 0;
        margin-bottom: 8px;
      }
      .hero p {
        margin-bottom: 0;
        opacity: 0.92;
        color: #b8c9ea;
      }
      .glass-card {
        background: linear-gradient(145deg, rgba(24,38,62,0.88), rgba(15,25,43,0.84));
        border: 1px solid rgba(108,134,188,0.3);
        border-radius: 18px;
        box-shadow: 0 10px 25px rgba(2,8,18,0.45);
        backdrop-filter: blur(12px);
        padding: 14px 14px 8px 14px;
        margin-bottom: 14px;
      }
      .input-row {
        display: flex;
        flex-wrap: wrap;
      }
      .equal-row {
        display: flex;
        flex-wrap: wrap;
      }
      .equal-col {
        display: flex;
      }
      .equal-col > .glass-card {
        width: 100%;
        display: flex;
        flex-direction: column;
      }
      .equal-col .table-shell {
        flex: 1;
      }
      .input-column {
        display: flex;
      }
      .input-column .glass-card {
        width: 100%;
        min-height: 560px;
      }
      .glass-title {
        font-family: 'Space Grotesk', sans-serif;
        font-weight: 700;
        letter-spacing: 0.2px;
        color: #e6eeff;
        margin: 2px 0 10px 0;
      }
      .control-label {
        color: #adc3eb;
        font-weight: 700;
      }
      .form-control, .selectize-input, .irs {
        border-radius: 12px !important;
      }
      .form-control,
      .selectize-input,
      .selectize-dropdown,
      .irs--shiny .irs-line,
      .irs--shiny .irs-grid-text,
      .irs--shiny .irs-single {
        background: #0f1c34 !important;
        color: #d9e7ff !important;
        border: 1px solid rgba(112,139,193,0.45) !important;
      }
      .selectize-input input {
        color: #d9e7ff !important;
      }
      .btn-primary, .btn-info {
        border: none;
        border-radius: 12px;
        font-weight: 700;
      }
      .btn-primary {
        background: linear-gradient(135deg, #2f80ed, #56ccf2);
        color: #051226;
      }
      .btn-info {
        background: linear-gradient(135deg, #f2994a, #f2c94c);
        color: #1d1a0f;
      }
      .kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 10px;
        margin-bottom: 12px;
      }
      .kpi-card {
        border-radius: 14px;
        background: rgba(25,40,67,0.9);
        border: 1px solid rgba(102,129,182,0.4);
        padding: 12px;
        transition: transform 0.22s ease, box-shadow 0.22s ease, background 0.22s ease;
      }
      .kpi-card:hover {
        transform: translateY(-3px) scale(1.02);
        box-shadow: 0 14px 28px rgba(8,15,29,0.52);
        background: rgba(33,52,86,0.96);
      }
      .kpi-title {
        color: #9ab5e3;
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.7px;
      }
      .kpi-value {
        color: #f0f6ff;
        font-size: 20px;
        font-weight: 800;
        line-height: 1.2;
      }
      .kpi-subtitle {
        color: #9eb6df;
        font-size: 12px;
      }
      .kpi-help {
        margin-top: 6px;
        font-size: 12px;
        line-height: 1.35;
        color: #b8cbee;
        opacity: 0;
        max-height: 0;
        overflow: hidden;
        transition: opacity 0.2s ease, max-height 0.2s ease;
      }
      .kpi-card:hover .kpi-help {
        opacity: 0.95;
        max-height: 84px;
      }
      .hover-card {
        transition: transform 0.22s ease, box-shadow 0.22s ease, background 0.22s ease;
      }
      .hover-card:hover {
        transform: translateY(-4px) scale(1.01);
        box-shadow: 0 14px 30px rgba(8,15,29,0.54);
        background: linear-gradient(145deg, rgba(34,53,87,0.95), rgba(18,30,51,0.92));
      }
      .hover-help {
        margin-top: -2px;
        margin-bottom: 10px;
        color: #b8cbee;
        font-size: 12px;
        line-height: 1.35;
        opacity: 0;
        max-height: 0;
        overflow: hidden;
        transition: opacity 0.2s ease, max-height 0.2s ease;
      }
      .hover-card:hover .hover-help {
        opacity: 0.95;
        max-height: 90px;
      }
      .quick-actions-card {
        padding-bottom: 14px;
      }
      .quick-actions-intro {
        color: #aec4ea;
        font-size: 13px;
        margin-bottom: 10px;
      }
      .quick-actions-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 8px;
      }
      .quick-action-pill {
        background: rgba(24,38,63,0.94);
        border: 1px solid rgba(106,132,186,0.4);
        border-radius: 12px;
        padding: 9px 10px;
      }
      .quick-action-label {
        display: block;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        color: #98b2df;
        font-weight: 700;
      }
      .quick-action-value {
        display: block;
        color: #f0f6ff;
        font-weight: 800;
        font-size: 15px;
      }
      .quick-action-note {
        color: #b8cbee;
        font-size: 12px;
        margin-top: 8px;
      }
      .table-shell {
        background: rgba(18,30,51,0.86);
        border: 1px solid rgba(106,132,186,0.35);
        border-radius: 14px;
        padding: 10px;
        overflow-x: visible;
      }
      .table-note {
        color: #aec4ea;
        font-size: 12px;
        margin: 2px 0 8px 0;
      }
      .shiny-plot-output {
        background: transparent !important;
      }
      .summary-list {
        display: grid;
        grid-template-columns: repeat(4, minmax(150px, 1fr));
        gap: 8px;
      }
      .summary-item {
        display: block;
        background: rgba(27,42,69,0.9);
        border: 1px solid rgba(106,132,186,0.38);
        border-radius: 10px;
        padding: 9px 10px;
      }
      .summary-label {
        color: #98b2df;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-weight: 700;
      }
      .summary-value {
        color: #f0f6ff;
        font-size: 32px;
        font-weight: 800;
        line-height: 1.2;
        margin-top: 4px;
      }
      #prediction_table table {
        width: 100%;
        min-width: 980px;
        border-collapse: collapse;
        overflow: hidden;
        border-radius: 12px;
      }
      #prediction_table thead th {
        background: rgba(42,64,100,0.88);
        color: #f0f6ff;
        font-weight: 800;
        border-bottom: 1px solid rgba(123,152,205,0.3);
        padding: 9px;
      }
      #prediction_table tbody td {
        color: #d7e6ff;
        border-bottom: 1px solid rgba(106,132,186,0.25);
        padding: 9px;
      }
      #prediction_table tbody tr:hover {
        background: rgba(39,58,94,0.72);
      }
      #decision_diagnostics_table table {
        width: 100%;
        min-width: 0;
        table-layout: fixed;
        border-collapse: collapse;
        overflow: hidden;
        border-radius: 12px;
      }
      #decision_diagnostics_table thead th {
        background: rgba(42,64,100,0.88);
        color: #f0f6ff;
        font-weight: 800;
        border-bottom: 1px solid rgba(123,152,205,0.3);
        padding: 9px;
      }
      #decision_diagnostics_table tbody td {
        color: #d7e6ff;
        border-bottom: 1px solid rgba(106,132,186,0.25);
        padding: 9px;
        word-break: break-word;
      }
      #decision_diagnostics_table tbody tr:hover {
        background: rgba(39,58,94,0.72);
      }
      .modal-content {
        border-radius: 16px;
        border: 1px solid rgba(106,132,186,0.4);
        background: linear-gradient(145deg, rgba(20,32,54,0.95), rgba(13,22,40,0.92));
      }
      .modal-header h4 {
        font-family: 'Space Grotesk', sans-serif;
        color: #e6eeff;
      }
      .shiny-output-error-validation {
        color: #8b2d2d;
        font-weight: 700;
      }
      @media (max-width: 767px) {
        .hero { padding: 18px; }
        .input-row {
          display: block;
        }
        .equal-row {
          display: block;
        }
        .equal-col {
          display: block;
        }
        .input-column {
          display: block;
        }
        .input-column .glass-card {
          min-height: auto;
        }
        .summary-list {
          grid-template-columns: repeat(2, minmax(130px, 1fr));
        }
        .table-shell {
          overflow-x: auto;
        }
        #prediction_table table,
        #decision_diagnostics_table table {
          min-width: 0;
        }
      }
      @media (max-width: 480px) {
        .summary-list {
          grid-template-columns: 1fr;
        }
      }
    ")),
    tags$script(HTML(" 
      Shiny.addCustomMessageHandler('scrollToOutputs', function(message) {
        var outputSection = document.getElementById('output_section');
        if (outputSection) {
          setTimeout(function() {
            outputSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
          }, 120);
        }
      });
    "))
  ),
  div(
    class = "app-shell",
    div(
      class = "hero",
      h2("Customer Churn Intelligence & Intervention System"),
      p("Glass-style real-time retention command center with prediction and intervention intelligence.")
    ),
    fluidRow(
      class = "input-row",
      column(
        6,
        class = "input-column",
        div(
          class = "glass-card",
          h4(class = "glass-title", "Customer Input - Profile"),
          selectInput("customer_id", "Existing Customer", choices = NULL),
          checkboxInput("new_customer", "Score as New Customer", value = FALSE),
          numericInput("age", "Age", value = 35, min = 18, max = 90),
          numericInput("tenure_months", "Tenure (Months)", value = 24, min = 0, max = 240),
          numericInput("monthly_charges", "Monthly Charges", value = 79.9, min = 1),
          numericInput("support_tickets", "Support Tickets", value = 1, min = 0),
          numericInput("last_login_days", "Days Since Last Login", value = 5, min = 0)
        )
      ),
      column(
        6,
        class = "input-column",
        div(
          class = "glass-card",
          h4(class = "glass-title", "Customer Input - Engagement & Actions"),
          selectInput("contract_type", "Contract Type", choices = c("month_to_month", "annual", "biannual")),
          selectInput("payment_method", "Payment Method", choices = c("credit_card", "bank_transfer", "wallet", "cash")),
          sliderInput("usage_score", "Usage Score", min = 0, max = 100, value = 60),
          sliderInput("satisfaction_score", "Satisfaction Score", min = 0, max = 100, value = 70),
          numericInput("sim_batch", "Simulation Batch Size", value = cfg$simulation$batch_size, min = 1, max = 500),
          fluidRow(
            column(6, actionButton("predict_btn", "Run Prediction", class = "btn-primary btn-block")),
            column(6, actionButton("simulate_btn", "Simulate Batch", class = "btn-info btn-block"))
          )
        )
      )
    ),
    div(
      id = "output_section",
      conditionalPanel(
        condition = "output.output_ready",
        uiOutput("headline_kpis"),
        div(
          class = "glass-card hover-card quick-actions-card",
          h4(class = "glass-title", "Quick Recommendation"),
          p(class = "hover-help", "Summary of what to do for the latest scored customer and why this action is financially sensible."),
          uiOutput("recommendation_text")
        ),
        fluidRow(
          class = "equal-row",
          column(
            5,
            class = "equal-col",
            div(
              class = "glass-card hover-card",
              h4(class = "glass-title", "Decision Diagnostics"),
              p(class = "hover-help", "Compares expected loss for all actions for the latest customer so recommendation logic is fully transparent."),
              div(
                class = "table-shell",
                p(class = "table-note", "Lower expected loss is better. Break-even threshold is action cost divided by action effectiveness."),
                tableOutput("decision_diagnostics_table")
              )
            )
          ),
          column(
            7,
            class = "equal-col",
            div(
              class = "glass-card hover-card",
              h4(class = "glass-title", "Latest Scored Customer"),
              p(class = "hover-help", "Single-customer snapshot with the key financial and intervention fields from the latest scoring event."),
              div(
                class = "table-shell",
                p(class = "table-note", "Use this as a quick QA view before triggering manual intervention workflows."),
                uiOutput("prediction_summary_ui")
              )
            )
          )
        ),
        fluidRow(
          column(
            6,
            div(
              class = "glass-card hover-card",
              h4(class = "glass-title", "Risk Profile"),
              p(class = "hover-help", "Shows how the customer base is distributed across Low, Medium, and High churn risk bands to prioritize retention attention."),
              plotOutput("risk_plot", height = "280px")
            )
          ),
          column(
            6,
            div(
              class = "glass-card hover-card",
              h4(class = "glass-title", "Action Mix"),
              p(class = "hover-help", "Displays the portfolio split of recommended interventions so teams can plan effort and campaign capacity."),
              plotOutput("action_plot", height = "280px")
            )
          )
        )
      )
    )
  )
)

server <- function(input, output, session) {
  latest_view <- reactiveVal(get_latest_customer_view(conn))
  latest_prediction <- reactiveVal(NULL)
  output_ready_state <- reactiveVal(FALSE)

  output$output_ready <- reactive({
    output_ready_state()
  })
  outputOptions(output, "output_ready", suspendWhenHidden = FALSE)

  portfolio_stats <- reactive({
    view <- latest_view()
    if (nrow(view) == 0) {
      return(list(
        customers = 0,
        total_loss = 0,
        avg_churn = NA_real_,
        avg_clv = NA_real_,
        avg_time = NA_real_
      ))
    }

    list(
      customers = nrow(view),
      total_loss = sum(view$expected_loss, na.rm = TRUE),
      avg_churn = mean(view$churn_probability, na.rm = TRUE),
      avg_clv = mean(view$predicted_clv, na.rm = TRUE),
      avg_time = mean(view$expected_time_to_churn, na.rm = TRUE)
    )
  })

  observe({
    customers <- get_customer_data(conn)
    if (nrow(customers) > 0) {
      updateSelectInput(session, "customer_id", choices = customers$customer_id, selected = customers$customer_id[[1]])
    }
  })

  observeEvent(input$customer_id, {
    if (isTRUE(input$new_customer)) {
      return(invisible(NULL))
    }

    customers <- get_customer_data(conn)
    selected <- customers[customers$customer_id == as.integer(input$customer_id), , drop = FALSE]

    if (nrow(selected) == 1) {
      updateNumericInput(session, "age", value = selected$age)
      updateNumericInput(session, "tenure_months", value = selected$tenure_months)
      updateNumericInput(session, "monthly_charges", value = selected$monthly_charges)
      updateNumericInput(session, "support_tickets", value = selected$support_tickets)
      updateNumericInput(session, "last_login_days", value = selected$last_login_days)
      updateSelectInput(session, "contract_type", selected = selected$contract_type)
      updateSelectInput(session, "payment_method", selected = selected$payment_method)
      updateSliderInput(session, "usage_score", value = selected$usage_score)
      updateSliderInput(session, "satisfaction_score", value = selected$satisfaction_score)
    }
  }, ignoreInit = TRUE)

  observeEvent(input$predict_btn, {
    tryCatch(
      {
        customers <- get_customer_data(conn)

        if (isTRUE(input$new_customer) || length(input$customer_id) == 0 || is.na(input$customer_id)) {
          customer_id <- if (nrow(customers) == 0) 1 else (max(customers$customer_id, na.rm = TRUE) + 1)
          base_row <- data.frame(
            churned = 0,
            duration = 12,
            event = 0,
            monthly_margin = input$monthly_charges * 0.45,
            true_clv = input$monthly_charges * 12,
            stringsAsFactors = FALSE
          )
        } else {
          customer_id <- as.integer(input$customer_id)
          selected <- customers[customers$customer_id == customer_id, , drop = FALSE]

          if (nrow(selected) == 1) {
            base_row <- selected[, c("churned", "duration", "event", "monthly_margin", "true_clv"), drop = FALSE]
          } else {
            base_row <- data.frame(
              churned = 0,
              duration = 12,
              event = 0,
              monthly_margin = input$monthly_charges * 0.45,
              true_clv = input$monthly_charges * 12,
              stringsAsFactors = FALSE
            )
          }
        }

        new_row <- data.frame(
          customer_id = customer_id,
          age = as.integer(input$age),
          tenure_months = as.integer(input$tenure_months),
          monthly_charges = as.numeric(input$monthly_charges),
          support_tickets = as.integer(input$support_tickets),
          last_login_days = as.integer(input$last_login_days),
          contract_type = input$contract_type,
          payment_method = input$payment_method,
          usage_score = as.numeric(input$usage_score),
          satisfaction_score = as.numeric(input$satisfaction_score),
          is_active = as.integer(input$last_login_days <= 15),
          churned = base_row$churned,
          duration = base_row$duration,
          event = base_row$event,
          monthly_margin = base_row$monthly_margin,
          true_clv = base_row$true_clv,
          updated_at = as.character(Sys.time()),
          stringsAsFactors = FALSE
        )

        # Validate input
        validate_customer_input(new_row)

        upsert_customer_data(conn, new_row)
        scored <- run_prediction_pipeline(conn, new_row, models, cfg)

        latest_prediction(scored)
        latest_view(get_latest_customer_view(conn))
        output_ready_state(TRUE)

        session$onFlushed(function() {
          session$sendCustomMessage("scrollToOutputs", list())
        }, once = TRUE)
      },
      error = function(e) {
        warning_msg <- paste("Input Error:", e$message)
        log_event("Prediction error", e$message, is_error = TRUE)
        showNotification(warning_msg, type = "warning", duration = 5)
      }
    )
  })

  observeEvent(input$simulate_btn, {
    tryCatch(
      {
        scored <- simulate_realtime_updates(
          conn,
          models,
          cfg,
          batch_size = as.integer(input$sim_batch)
        )

        if (nrow(scored) > 0) {
          latest_prediction(scored[1, , drop = FALSE])
        }

        latest_view(get_latest_customer_view(conn))
        output_ready_state(TRUE)

        session$onFlushed(function() {
          session$sendCustomMessage("scrollToOutputs", list())
        }, once = TRUE)

        showNotification("Simulation batch completed", type = "message", duration = 3)
      },
      error = function(e) {
        log_event("Simulation error", e$message, is_error = TRUE)
        showNotification(paste("Simulation error:", e$message), type = "warning", duration = 5)
      }
    )
  })

  output$headline_kpis <- renderUI({
    stats <- portfolio_stats()

    div(
      class = "kpi-grid",
      build_kpi_card(
        "Portfolio Customers",
        format(stats$customers, big.mark = ","),
        description = "Total scored customers currently represented in the live analytics view."
      ),
      build_kpi_card(
        "Total Expected Loss",
        dollar(stats$total_loss),
        description = "Estimated near-term revenue at risk if no interventions are applied."
      ),
      build_kpi_card(
        "Average Churn",
        if (is.na(stats$avg_churn)) "N/A" else percent(stats$avg_churn, accuracy = 0.1),
        description = "Mean churn probability across the visible portfolio; higher means greater overall risk."
      ),
      build_kpi_card(
        "Average CLV",
        if (is.na(stats$avg_clv)) "N/A" else dollar(stats$avg_clv),
        description = "Average predicted customer lifetime value used to size intervention economics."
      ),
      build_kpi_card(
        "Avg Time-To-Churn",
        if (is.na(stats$avg_time)) "N/A" else paste0(round(stats$avg_time, 1), " months"),
        description = "Estimated remaining time window before churn, guiding urgency of action."
      )
    )
  })

  prediction_table_df <- reactive({
    pred <- latest_prediction()
    if (is.null(pred) || nrow(pred) == 0) {
      return(data.frame(Message = "Run prediction or simulation to view outputs"))
    }

    data.frame(
      customer_id = pred$customer_id,
      churn_probability = percent(pred$churn_probability, accuracy = 0.1),
      expected_time_to_churn = round(pred$expected_time_to_churn, 2),
      predicted_clv = dollar(pred$predicted_clv),
      expected_loss = dollar(pred$expected_loss),
      recommended_action = format_action_label(pred$recommended_action),
      expected_benefit = dollar(pred$expected_benefit),
      stringsAsFactors = FALSE
    )
  })

  decision_diagnostics_df <- reactive({
    pred <- latest_prediction()
    if (is.null(pred) || nrow(pred) == 0) {
      return(data.frame(Message = "Run prediction or simulation to view decision diagnostics"))
    }

    top <- pred[1, ]
    break_even_retention <- cfg$decision_engine$actions$retention_call$cost /
      cfg$decision_engine$actions$retention_call$effectiveness
    break_even_discount <- cfg$decision_engine$actions$discount$cost /
      cfg$decision_engine$actions$discount$effectiveness

    data.frame(
      Action = format_action_label(c("no_action", "retention_call", "discount")),
      `Expected Loss` = scales::dollar(c(top$loss_no_action, top$loss_retention_call, top$loss_discount)),
      `Break-even No Action Loss` = c(
        "N/A",
        scales::dollar(break_even_retention),
        scales::dollar(break_even_discount)
      ),
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  })

  portfolio_table_df <- reactive({
    view <- latest_view()
    if (nrow(view) == 0) {
      return(data.frame(Message = "No scored customers"))
    }

    out <- head(
      view[order(-view$expected_loss), c(
        "customer_id", "churn_probability", "expected_time_to_churn", "predicted_clv",
        "expected_loss", "recommended_action", "expected_benefit"
      )],
      20
    )

    out$churn_probability <- percent(out$churn_probability, accuracy = 0.1)
    out$predicted_clv <- dollar(out$predicted_clv)
    out$expected_loss <- dollar(out$expected_loss)
    out$expected_benefit <- dollar(out$expected_benefit)
    out$recommended_action <- format_action_label(out$recommended_action)
    out
  })

  financial_summary_df <- reactive({
    stats <- portfolio_stats()
    data.frame(
      Metric = c("Customers", "Total Expected Loss", "Average Churn", "Average Predicted CLV", "Average Time-to-Churn"),
      Value = c(
        format(stats$customers, big.mark = ","),
        dollar(stats$total_loss),
        if (is.na(stats$avg_churn)) "N/A" else percent(stats$avg_churn, accuracy = 0.1),
        if (is.na(stats$avg_clv)) "N/A" else dollar(stats$avg_clv),
        if (is.na(stats$avg_time)) "N/A" else paste0(round(stats$avg_time, 1), " months")
      ),
      stringsAsFactors = FALSE
    )
  })

  financial_plot_obj <- reactive({
    view <- latest_view()
    validate(need(nrow(view) > 0 && !all(is.na(view$expected_loss)), "No financial data available."))

    agg <- aggregate(
      expected_loss ~ recommended_action,
      data = view,
      FUN = function(x) sum(x, na.rm = TRUE)
    )

    agg$recommended_action <- factor(
      format_action_label(agg$recommended_action),
      levels = c("No Action", "Retention Call", "Discount")
    )

    ggplot(agg, aes(x = recommended_action, y = expected_loss, fill = recommended_action)) +
      geom_col(width = 0.62, show.legend = FALSE) +
      scale_fill_manual(values = c("No Action" = "#5b728a", "Retention Call" = "#f4b860", "Discount" = "#4cc9b0")) +
      scale_y_continuous(labels = scales::dollar) +
      labs(
        title = "Expected Revenue Loss by Recommended Action",
        x = "Action",
        y = "Expected Loss"
      ) +
      theme_minimal(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "#101a30", color = NA),
        panel.background = element_rect(fill = "#121f37", color = NA),
        panel.grid.major = element_line(color = "#2a3f63"),
        panel.grid.minor = element_line(color = "#203354"),
        axis.text = element_text(color = "#d7e6ff"),
        axis.title = element_text(face = "bold", color = "#c7d8f5"),
        plot.title = element_text(face = "bold", color = "#e6eeff")
      )
  })

  risk_plot_obj <- reactive({
    view <- latest_view()
    validate(need(nrow(view) > 0 && !all(is.na(view$churn_probability)), "No risk data available."))

    view$risk_band <- risk_band(view$churn_probability, cfg)
    view$risk_band <- factor(view$risk_band, levels = c("Low", "Medium", "High"), ordered = TRUE)
    risk_counts <- as.data.frame(table(view$risk_band), stringsAsFactors = FALSE)
    names(risk_counts) <- c("risk_band", "customers")

    ggplot(risk_counts, aes(x = risk_band, y = customers, fill = risk_band)) +
      geom_col(width = 0.62, show.legend = FALSE) +
      geom_text(aes(label = customers), vjust = -0.4, color = "#eef5ff", fontface = "bold", size = 4) +
      scale_fill_manual(values = c("High" = "#ff6b6b", "Medium" = "#f4b860", "Low" = "#4cc9b0")) +
      scale_x_discrete(drop = FALSE) +
      labs(
        title = "Risk Segmentation",
        x = "Risk Band",
        y = "Customers"
      ) +
      expand_limits(y = max(risk_counts$customers, na.rm = TRUE) * 1.12) +
      theme_minimal(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "#101a30", color = NA),
        panel.background = element_rect(fill = "#121f37", color = NA),
        panel.grid.major = element_line(color = "#2a3f63"),
        panel.grid.minor = element_line(color = "#203354"),
        axis.text = element_text(color = "#d7e6ff"),
        panel.grid.major.x = element_blank(),
        plot.title = element_text(face = "bold", color = "#e6eeff"),
        axis.title = element_text(face = "bold", color = "#c7d8f5")
      )
  })

  action_plot_obj <- reactive({
    view <- latest_view()
    validate(need(nrow(view) > 0 && !all(is.na(view$recommended_action)), "No action data available."))

    action_counts <- as.data.frame(table(view$recommended_action), stringsAsFactors = FALSE)
    names(action_counts) <- c("recommended_action", "customers")
    action_counts <- action_counts[action_counts$customers > 0, , drop = FALSE]
    action_counts$pct <- action_counts$customers / sum(action_counts$customers)
    action_counts$action_display <- format_action_label(action_counts$recommended_action)
    action_counts$label <- paste0(action_counts$action_display, " (", percent(action_counts$pct, accuracy = 0.1), ")")
    label_map <- setNames(action_counts$label, action_counts$recommended_action)

    ggplot(action_counts, aes(x = "mix", y = customers, fill = recommended_action)) +
      geom_col(width = 1, color = "#0f1a2f", linewidth = 0.4) +
      coord_polar(theta = "y") +
      geom_text(aes(label = ifelse(pct >= 0.08, percent(pct, accuracy = 1), "")),
                position = position_stack(vjust = 0.5), color = "#f2f8ff", fontface = "bold", size = 4) +
      scale_fill_manual(values = c("discount" = "#4cc9b0", "retention_call" = "#f4b860", "no_action" = "#5b728a"),
                        breaks = action_counts$recommended_action,
                        labels = label_map[action_counts$recommended_action]) +
      labs(
        title = "Intervention Recommendation Mix",
        x = NULL,
        y = NULL,
        fill = "Action Share"
      ) +
      theme_void(base_size = 13) +
      theme(
        plot.background = element_rect(fill = "#101a30", color = NA),
        legend.background = element_rect(fill = "#101a30", color = NA),
        legend.key = element_rect(fill = "#101a30", color = NA),
        plot.title = element_text(face = "bold", color = "#e6eeff"),
        legend.position = "right",
        legend.title = element_text(face = "bold", color = "#c7d8f5"),
        legend.text = element_text(color = "#d7e6ff")
      )
  })

  output$prediction_table <- renderTable({
    prediction_table_df()
  })

  output$prediction_summary_ui <- renderUI({
    pred <- latest_prediction()
    if (is.null(pred) || nrow(pred) == 0) {
      return(div(class = "quick-actions-intro", "Run prediction or simulation to view latest customer details."))
    }

    top <- pred[1, ]
    items <- list(
      list(label = "Customer ID", value = as.character(top$customer_id)),
      list(label = "Churn Probability", value = scales::percent(top$churn_probability, accuracy = 0.1)),
      list(label = "Expected Time To Churn", value = paste0(round(top$expected_time_to_churn, 2), " months")),
      list(label = "Predicted CLV", value = scales::dollar(top$predicted_clv)),
      list(label = "Expected Loss", value = scales::dollar(top$expected_loss)),
      list(label = "Recommended Action", value = format_action_label(top$recommended_action)),
      list(label = "Expected Benefit", value = scales::dollar(top$expected_benefit))
    )

    div(
      class = "summary-list",
      lapply(items, function(x) {
        div(
          class = "summary-item",
          span(class = "summary-label", x$label),
          span(class = "summary-value", x$value)
        )
      })
    )
  })

  output$decision_diagnostics_table <- renderTable({
    decision_diagnostics_df()
  })

  output$recommendation_text <- renderUI({
    pred <- latest_prediction()
    if (is.null(pred) || nrow(pred) == 0) {
      return(div(class = "quick-actions-intro", "No recommendation available yet. Score a customer or run simulation."))
    }

    top <- pred[1, ]
    risk_label <- risk_band(top$churn_probability, cfg)
    action_raw <- as.character(top$recommended_action)
    action_help <- switch(
      action_raw,
      discount = "Offer a targeted incentive to reduce immediate churn probability.",
      retention_call = "Route to a specialist call queue for proactive human intervention.",
      no_action = "Current economics suggest monitoring rather than immediate spend.",
      "Review this case manually before finalizing outreach."
    )

    div(
      p(class = "quick-actions-intro", "Latest decision summary and execution hint:"),
      div(
        class = "quick-actions-grid",
        div(class = "quick-action-pill", span(class = "quick-action-label", "Recommended Action"), span(class = "quick-action-value", format_action_label(action_raw))),
        div(class = "quick-action-pill", span(class = "quick-action-label", "Risk Band"), span(class = "quick-action-value", as.character(risk_label))),
        div(class = "quick-action-pill", span(class = "quick-action-label", "Churn Probability"), span(class = "quick-action-value", scales::percent(top$churn_probability, accuracy = 0.1))),
        div(class = "quick-action-pill", span(class = "quick-action-label", "Expected Revenue Loss"), span(class = "quick-action-value", scales::dollar(top$expected_loss))),
        div(class = "quick-action-pill", span(class = "quick-action-label", "Expected Benefit"), span(class = "quick-action-value", scales::dollar(top$expected_benefit))),
        div(class = "quick-action-pill", span(class = "quick-action-label", "Predicted CLV"), span(class = "quick-action-value", scales::dollar(top$predicted_clv)))
      ),
      p(class = "quick-action-note", paste("Why this action:", action_help))
    )
  })

  output$financial_plot <- renderPlot({
    print(financial_plot_obj())
  }, bg = "transparent")

  output$risk_plot <- renderPlot({
    print(risk_plot_obj())
  }, bg = "transparent")

  output$action_plot <- renderPlot({
    print(action_plot_obj())
  }, bg = "transparent")

  output$portfolio_table <- renderTable({
    portfolio_table_df()
  })

  output$financial_summary <- renderTable({
    financial_summary_df()
  })

}

shinyApp(ui = ui, server = server)
