# Demo Local DuckDB

This demo folder contains everything needed to run a small local version of the pipeline using dbt and DuckDB.

## What it does

- Uses `dbt-duckdb` adapter
- Loads sample seeds from `seeds/demo_application_train.csv` and `seeds/demo_bureau.csv`
- Builds staging models locally
- Runs dbt tests on the staging layer
- Stores the demo database at `demo/demo.duckdb`

## How to run

From `my_first_project`:

```bash
./run_demo.sh
```

On Windows PowerShell:

```powershell
./run_demo.ps1
```

## How it works

- `run_demo.sh` / `run_demo.ps1` create a Python virtual environment in `.venv_demo`
- Dependencies are installed from `requirements.txt`
- dbt profile is loaded from `demo/profiles.yml`
- dbt seed, run, and test are executed against DuckDB

## Notes

- This demo is intentionally minimal: it only runs the staging layer and tests the transformed raw data.
- For full BigQuery execution, use the main `README.md` instructions and a BigQuery profile.
