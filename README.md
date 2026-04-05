# Customer Churn Intelligence & Intervention System (R)

Production-grade modular R platform for churn prediction, survival analysis, CLV estimation, expected loss quantification, and intervention recommendation.

The application combines model-driven scoring with an interactive Shiny experience for analysts and retention teams.

## What This System Does

- Predicts churn probability using an ensemble of:
  - Logistic Regression
  - Random Forest
- Estimates time-to-churn using a Cox Proportional Hazards model
- Estimates CLV using margin and discounted expected time horizon
- Computes expected loss:
  - `expected_loss = churn_probability * predicted_clv`
- Recommends interventions by comparing expected post-action loss for:
  - `Discount`
  - `Retention Call`
  - `No Action`
- Supports real-time simulation batches for continuous testing
- Persists all scoring outputs in SQLite for traceability

## Current Decision Logic

For each customer and action:

- `adjusted_churn = churn_probability * (1 - effectiveness)`
- `adjusted_loss = adjusted_churn * predicted_clv + action_cost`

Recommendation is the action with minimum `adjusted_loss`, with expected benefit computed against no-action loss.

Configured intervention economics (in `config/config.yml`):

- `discount`: cost `22`, effectiveness `0.30`
- `retention_call`: cost `8`, effectiveness `0.18`
- `no_action`: cost `0`, effectiveness `0.00`

## Dashboard UX (Current)

- Initial view shows input controls first
- Outputs are generated and revealed after scoring/simulation
- Auto-scroll jumps to result section after action
- Top KPI cards include hover expansion with explanations
- `Decision Diagnostics` shows action-wise expected loss and break-even context
- `Latest Scored Customer` uses a compact metric-card layout (no wide row table)
- Risk chart is ordered `Low -> Medium -> High`
- Action mix chart uses cleaned labels (`No Action`, `Retention Call`, `Discount`)

## Important UI Semantics

- `Existing Customer` input = selected source profile in the form
- `Latest Scored Customer` = most recent scoring event output

These can differ when simulation is run or when scoring as a new customer.

## Project Structure

- `app.R`: Shiny application (UI + server)
- `config/config.yml`: environment + economic parameters
- `R/config.R`: config loader
- `R/db.R`: SQLite schema and data access
- `R/data_ingestion.R`: CSV ingestion and feature engineering
- `R/data_generation.R`: synthetic data generation and update simulation
- `R/modeling.R`: model training/loading/inference and scoring math
- `R/decision_engine.R`: intervention recommendation policy
- `R/pipeline.R`: end-to-end prediction and persistence pipeline
- `R/simulation.R`: real-time update simulation logic
- `R/bootstrap.R`: one-shot initialization and baseline scoring
- `scripts/install_packages.R`: dependency installer
- `scripts/train_and_bootstrap.R`: bootstrap script
- `scripts/simulate_stream.R`: stream simulation runner

## Setup

1. Install dependencies:

```bash
Rscript scripts/install_packages.R
```

2. Bootstrap data and models:

```bash
Rscript scripts/train_and_bootstrap.R
```

3. Run dashboard (default R on PATH):

```bash
Rscript -e "shiny::runApp('app.R', launch.browser = TRUE)"
```

4. Run dashboard with explicit R 4.4.3 path (Windows):

```powershell
& "D:/R-4.4.3/bin/Rscript.exe" -e "shiny::runApp('app.R', launch.browser = TRUE)"
```

Optional stream simulation:

```bash
Rscript scripts/simulate_stream.R 20 50 0.5
```

Arguments:

- `iterations` (default `10`)
- `batch_size` (default from config)
- `sleep_seconds` (default `1`)

## Persistence Tables (SQLite)

- `customer_data`
- `predictions`
- `survival_estimates`
- `clv_values`
- `intervention_decisions`
- `audit_log`

## Notes

- Models are stored in `models/` as `.rds` artifacts.
- The system supports repeated scoring cycles and monitoring over time.
- Action labels in UI are normalized to readable Title Case.
- Most thresholds/costs are configurable in `config/config.yml`.
