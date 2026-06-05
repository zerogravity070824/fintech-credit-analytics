# Fintech Credit Analytics: Scalable Data Pipeline for Loan Risk Monitoring

![dbt CI/CD](https://github.com/zerogravity070824/fintech-credit-analytics/actions/workflows/dbt_run.yml/badge.svg)

![dbt](https://img.shields.io/badge/dbt-1.8.2-orange?logo=dbt)
![BigQuery](https://img.shields.io/badge/BigQuery-Google_Cloud-4285F4?logo=google-cloud)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python)
![CI/CD](https://github.com/zerogravity070824/fintech-credit-analytics/actions/workflows/dbt_run.yml/badge.svg)

## Project Overview

This pipeline simulates an **end-to-end ELT data transformation** for a financial institution (Fintech / Multi-finance). Raw credit history data is transformed into analytics-ready models consumed by the Risk and Business Intelligence teams.

**Business Context:**
Risk Analysts need daily visibility into credit portfolio performance. This pipeline automates the entire data transformation process — from raw layer to mart layer — enabling the team to monitor key metrics without repetitive manual queries.

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Cloud Data Warehouse | Google BigQuery | — |
| Data Transformation | dbt Core + dbt-bigquery | 1.8.2 |
| Data Quality | dbt tests + dbt_utils + dbt_expectations | — |
| Orchestration & CI/CD | GitHub Actions | — |
| Visualization | Looker Studio | — |
| Language | SQL, YAML, Python | Python 3.10 |

---

## Data Architecture

```
┌─────────────┐      ┌──────────────────────────────────────────────────────────────┐
│  CSV Data    │      │                    Google BigQuery                           │
│  (Kaggle)    │      │                                                              │
│              │─────▶│  ┌────────────┐    ┌──────────────┐    ┌─────────────────┐   │
│              │  py  │  │ Raw Layer  │───▶│   Staging    │───▶│  Intermediate   │   │
│              │      │  │            │ dbt│  stg_loans   │ dbt│  int_credit_    │   │
└─────────────┘      │  │ application│    │  stg_bureau  │    │  profile        │   │
                      │  │ _train     │    └──────────────┘    └────────┬────────┘   │
                      │  │ bureau     │                                 │             │
                      │  └────────────┘                                 ▼             │
                      │                                        ┌───────────────┐     │
                      │                                        │    Marts      │     │
                      │                                        │  dim_clients  │     │
                      │                                        │  fct_loan_    │     │
                      │                                        │  applications │     │
                      │                                        │  obt_credit_  │──┐  │
                      │                                        │  risk         │  │  │
                      │                                        └───────────────┘  │  │
                      └──────────────────────────────────────────────────────────┼──┘
                                                                                 │
┌────────────────────┐     ┌────────────────────────┐                            │
│  GitHub Actions    │     │   Looker Studio        │◀───────────────────────────┘
│  CI/CD Pipeline    │     │   Dashboard            │
│  ─ dbt run (daily) │     │   ─ Default Rate       │
│  ─ dbt test        │     │   ─ DTI Distribution   │
│  ─ source freshness│     │   ─ Portfolio Analysis  │
└────────────────────┘     └────────────────────────┘
```

---

## Results & Impact
- **Automation:** Reduced manual query time from ~2 hours/day to fully automated refresh
- **Data Quality:** 6 automated test layers catch anomalies before they reach dashboard
- **Scalability:** Pipeline handles 30,000+ loan applications with daily incremental refresh

---

## Data Modeling (Star Schema)

The pipeline follows a modular dbt approach with a three-layer structure:

**Staging Layer (`stg_`)** — Cleans raw data, standardizes column naming to *snake_case*, handles data type casting, and resolves known anomalies (e.g., `days_employed = 365243` → `NULL`).

**Intermediate Layer (`int_`)** — Joins client profiles (`stg_loans`) with external bureau credit history (`stg_bureau`), aggregates credit history per applicant, and calculates the **Debt-to-Income Ratio (DTI)** as the primary risk metric.

**Marts Layer (`dim_`, `fct_`, `obt_`)** — Builds Dimension and Fact tables following star schema design, then denormalizes into a **One Big Table (`obt_credit_risk`)** optimized for Looker Studio dashboard performance.

### Key Columns: `obt_credit_risk`

| Column | Type | Description |
|---|---|---|
| `application_id` | STRING | Unique primary key per loan application |
| `is_default` | INT64 | Default label: 0 = Performing, 1 = Default |
| `contract_type` | STRING | Loan contract type |
| `total_income_idr` | NUMERIC | Total applicant income |
| `loan_amount_idr` | NUMERIC | Requested loan amount |
| `loan_annuity_idr` | NUMERIC | Monthly loan installment |
| `debt_to_income_ratio` | FLOAT64 | Installment-to-income ratio (DTI) |
| `total_previous_loans` | INT64 | Number of previous credit records in other banks |
| `total_bureau_debt_idr` | NUMERIC | Total outstanding debt in other banks |
| `gender` | STRING | Applicant gender |
| `owns_car` | STRING | Car ownership flag |
| `owns_realty` | STRING | Real estate ownership flag |
| `total_children` | INT64 | Number of dependent children |
| `income_type` | STRING | Employment income type |
| `education_level` | STRING | Education level |
| `family_status` | STRING | Marital status |
| `housing_type` | STRING | Housing type |
| `age_years` | INT64 | Applicant age in years |
| `years_employed` | INT64 | Years of employment |

---

## Data Quality & Testing

This project implements multiple layers of data validation:

| Test Type | Description | Example |
|---|---|---|
| **Schema Tests** | Primary key uniqueness & not-null | `application_id` is unique and not null |
| **Referential Integrity** | Foreign key relationship validation | `bureau.SK_ID_CURR` → `application_train.SK_ID_CURR` |
| **Accepted Values** | Enum / domain value checks | `is_default` must be 0 or 1 |
| **Custom Singular Test** | Business-rule assertions | Loan amount must be > 0 |
| **dbt_utils** | Range checks, expression validation | DTI ratio within expected range |
| **Source Freshness** | Ensures raw data is up-to-date | Checked before each pipeline run |

---

## How to Run Locally

### Prerequisites
- Python 3.10+
- Google Cloud SDK
- Service Account with BigQuery Editor & BigQuery Job User roles

### 1. Clone & Install Dependencies

```bash
git clone https://github.com/zerogravity070824/fintech-credit-analytics.git
cd fintech-credit-analytics
pip install -r requirements.txt
```

### 2. Install dbt Packages

```bash
dbt deps
```

### 3. Setup `profiles.yml`

This file is **not committed** to the repository (listed in `.gitignore`). Create it manually at `~/.dbt/profiles.yml`:

```yaml
fintech_credit_analytics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: dbt_staging
      location: asia-southeast2
      keyfile: /path/to/your/service-account-key.json
      threads: 4
      timeout_seconds: 300
```

### 4. Verify Connection

```bash
dbt debug
```

All checks should return `OK` before proceeding.

### 5. Run the Pipeline

```bash
# Check source data freshness
dbt source freshness

# Run all models
dbt run

# Validate data quality
dbt test
```

### 6. Run Specific Models (Optional)

```bash
# Staging layer only
dbt run --select staging

# Specific model with all upstream dependencies
dbt run --select +obt_credit_risk

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## CI/CD Pipeline

The pipeline is executed via **GitHub Actions** with three trigger mechanisms:

- **On push to `main`** — runs automatically on every code change
- **Scheduled cron (06:00 WITA / 22:00 UTC)** — daily batch to ensure data freshness
- **Manual `workflow_dispatch`** — for on-demand runs

**Execution order:**

1. Authenticate to Google Cloud using a Service Account stored in GitHub Secrets
2. `dbt deps` — install dbt packages
3. `dbt source freshness` — validate source data freshness before transformation
4. `dbt run` — execute models and update BigQuery tables **(CD)**
5. `dbt test` — validate data quality: no nulls on PKs, no duplicates, anomaly checks **(CI)**

Python dependency caching is enabled to reduce build time.

**Required GitHub Secrets:**

| Secret | Description |
|---|---|
| `GCP_SA_KEY` | GCP Service Account JSON key |
| `GCP_PROJECT_ID` | Google Cloud Project ID |
| `DBT_DATASET` | Target BigQuery dataset name |

---

## Dashboard & Visualization

Data from `obt_credit_risk` is connected directly to Looker Studio for portfolio monitoring.

**[→ View Looker Studio Dashboard](https://lookerstudio.google.com/reporting/30bb83de-cf37-4263-a1d2-b05fccebd04b)**

Key metrics monitored:
- Default Rate (performing vs. defaulted loan segmentation)
- Portfolio Distribution by Income Type & Education Level
- Debt-to-Income Ratio Distribution
- Total Applicants & Loan Amount Distribution
- Bureau Debt & Previous Loans Analysis

---

## Project Structure

```
fintech-credit-analytics/
├── .github/workflows/
│   └── dbt_run.yml              # CI/CD pipeline (GitHub Actions)
├── models/
│   ├── staging/
│   │   ├── src_p2p_lending.yml  # Source definitions & tests
│   │   ├── stg_loans.sql        # Staging: loan applications
│   │   └── stg_bureau.sql       # Staging: bureau credit history
│   ├── intermediate/
│   │   └── int_credit_profile.sql  # Joins + DTI calculation
│   └── marts/core/
│       ├── dim_clients.sql      # Dimension: client demographics
│       ├── fct_loan_applications.sql  # Fact: loan transactions
│       ├── obt_credit_risk.sql  # One Big Table for dashboards
│       └── schema.yml           # Marts-level tests
├── macros/
│   └── cents_to_idr.sql         # Reusable currency formatting macro
├── snapshots/
│   └── snap_dim_clients.sql     # SCD Type 2 snapshot for client changes
├── tests/
│   └── assert_loan_amount_is_positive.sql  # Custom business rule test
├── dbt_project.yml              # dbt project configuration
├── packages.yml                 # dbt package dependencies
├── ingest_to_bq.py              # Python ingestion script (CSV → BigQuery)
├── requirements.txt             # Python dependencies
└── README.md
```

---

*Built by Ilham — 2026*