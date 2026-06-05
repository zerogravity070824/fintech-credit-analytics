# Repository Improvement Checklist for Internship Readiness

This checklist is focused on making the repo stronger for a Data Analyst / BI internship application, with security, documentation, reproducibility, and reviewability in mind.

## 1. Security & repo hygiene

- [x] Remove any sensitive credentials from the repository.
- [x] Ensure `.gitignore` excludes `*.json`, `*.csv`, `venv/`, and `__pycache__/`.
- [x] Add a short note in `README.md` that service account keys are not shipped with the repo.
- [x] Rotate exposed keys in GCP and purge the old key from any Git history if the repo was pushed with it.

## 2. Bug fix / data pipeline correctness

- [x] Fix `models/staging/stg_loans.sql` syntax bug: `SELECT * FROM renamed_and_casteddbt` → `SELECT * FROM renamed_and_casted`.
- [ ] Review all staging and intermediate models for consistent naming, casting, and null handling.
- [ ] Confirm `debt_to_income_ratio` calculation uses monthly income (`total_income_idr / 12`) throughout the pipeline.
- [ ] Validate the DTI range tests and source freshness expectations with actual dataset values.

## 3. README & project narrative

- [ ] Add a concise project summary at the top of `README.md` that states:
  - business problem solved,
  - your role / contributions,
  - the main technologies used.
- [ ] Add a short "Key results" or "Impact" section with measurable outcomes, even if estimates.
- [ ] Add a small architecture callout like "staging → intermediate → marts" and mention the star schema.
- [ ] Add one example of how a hiring manager can run a limited demo locally.
- [ ] Mention tools used for testing and data quality explicitly, e.g. `dbt test`, `dbt source freshness`, `dbt docs`.

## 4. Reproducibility & demo capability

- [x] Add a minimal `seeds/` dataset or small CSV sample so reviewers can run the pipeline without full GCP access.
- [x] Add a `run_demo.sh` or `Makefile` to automate steps:
  - `pip install -r requirements.txt`
  - `dbt deps`
  - `dbt debug`
  - `dbt run --select staging`
  - `dbt test --select stg_loans stg_bureau`
- [ ] Document how to use `dbt run --select +obt_credit_risk` and `dbt test --select +obt_credit_risk`.

## 5. CI/CD and code review readiness

- [ ] Add `pull_request` trigger to GitHub workflow so PRs run dbt tests before merge.
- [ ] Cache `~/.cache/pip` in GitHub Actions to speed repeated builds.
- [ ] Consider adding an optional `dbt docs generate` step or separate workflow for docs publishing.
- [ ] If using `requirements.txt`, install it instead of only `dbt-bigquery` in CI.

## 6. Data quality & testing

- [ ] Confirm source tests on raw columns, staging tests on transformed keys, and model tests on business rules.
- [ ] Add at least one custom dbt test or expectation that reflects a business sanity check beyond schema tests.
- [ ] Add a small narrative describing why `application_id`, `is_default`, `debt_to_income_ratio`, and `total_bureau_debt_idr` are important.

## 7. Portfolio polish

- [ ] Include a screenshot or link to the dashboard/viewer if available.
- [ ] Add a short "Why this project is relevant for DA/BI" section in the README.
- [ ] Add a `LICENSE` file if you want to show open-source readiness.
- [ ] Add a `CONTRIBUTING.md` or short note if you expect others to open PRs.

## 8. Optional but valuable

- [ ] Add a notebook or one-page summary of insights from the dataset.
- [ ] Add an example query or SQL snippet that a BI analyst would use to power the dashboard.
- [ ] Add a `dbt docs` artifact link if generated somewhere externally.

---

## Notes

- This repo already has good structure: staging, intermediate, marts, models, macros, tests, snapshots.
- The main improvements are documentation, reproducibility, and a stronger story for hiring managers.
- If you want, I can also help create `run_demo.sh` plus a tiny demo seed dataset.
