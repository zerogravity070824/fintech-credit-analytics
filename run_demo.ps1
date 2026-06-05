$ErrorActionPreference = 'Stop'
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $PSScriptRoot

if (-Not (Test-Path -Path '.venv_demo')) {
    python -m venv .venv_demo
}

$activatePath = Join-Path '.venv_demo' 'Scripts\Activate.ps1'
if (Test-Path $activatePath) {
    & $activatePath
} else {
    Write-Error 'Virtual environment activation script not found.'
    exit 1
}

python -m pip install --upgrade pip
pip install -r requirements.txt

$env:DBT_PROFILES_DIR = Join-Path $PSScriptRoot 'demo'
$venvDbt = Join-Path $PSScriptRoot '.venv_demo\Scripts\dbt.exe'

echo 'Installing dbt packages...'
& $venvDbt deps --project-dir . --profiles-dir $env:DBT_PROFILES_DIR

echo 'Seeding demo raw data...'
& $venvDbt seed --project-dir . --profiles-dir $env:DBT_PROFILES_DIR --select demo_application_train demo_bureau

echo 'Running staging models...'
& $venvDbt run --project-dir . --profiles-dir $env:DBT_PROFILES_DIR --select staging

echo 'Running tests on staging models...'
& $venvDbt test --project-dir . --profiles-dir $env:DBT_PROFILES_DIR --select staging

echo 'Local demo completed successfully.'
