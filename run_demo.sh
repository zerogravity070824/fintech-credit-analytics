#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [ ! -d ".venv_demo" ]; then
  python -m venv .venv_demo
fi

source .venv_demo/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

export DBT_PROFILES_DIR="$ROOT_DIR/demo"
DBT_BIN="$ROOT_DIR/.venv_demo/bin/dbt"

echo "Installing dbt packages..."
"$DBT_BIN" deps --project-dir . --profiles-dir "$DBT_PROFILES_DIR"

echo "Seeding demo raw data..."
"$DBT_BIN" seed --project-dir . --profiles-dir "$DBT_PROFILES_DIR" --select demo_application_train demo_bureau

echo "Running staging models..."
"$DBT_BIN" run --project-dir . --profiles-dir "$DBT_PROFILES_DIR" --select staging

echo "Running tests on staging models..."
"$DBT_BIN" test --project-dir . --profiles-dir "$DBT_PROFILES_DIR" --select staging

echo "Local demo completed successfully."
