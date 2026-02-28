#!/usr/bin/env bash
set -euo pipefail

# Start Airflow locally for testing in this repository.
# - Uses `airflow standalone` when available (recommended)
# - Otherwise runs `airflow db init`, creates an admin user, starts scheduler and webserver
# Usage: ./scripts/start_airflow_local.sh

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Use repo root as default AIRFLOW_HOME so Airflow reads DAGs directly from repo
AIRFLOW_HOME="${AIRFLOW_HOME:-$REPO_ROOT}"
export AIRFLOW_HOME
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
PORT="${AIRFLOW_PORT:-8080}"
# Airflow 3 workers call the Execution API. If UI/API port is customized,
# point workers to the same port to avoid queued->failed "state mismatch".
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="${AIRFLOW__CORE__EXECUTION_API_SERVER_URL:-http://localhost:${PORT}/execution/}"

mkdir -p "$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/logs"

if ! command -v airflow >/dev/null 2>&1; then
  echo "airflow command not found. Activate your virtualenv/conda or install Airflow." >&2
  exit 1
fi

# Ensure a local MongoDB connection exists for DAGs that expect `mongodb_default`.
# This is safe to run multiple times; the command will succeed or be ignored.
if airflow connections get mongodb_default -o json 2>/dev/null | grep -q '"conn_type": "mongodb"'; then
  echo "Recreating mongodb_default with conn_type=mongo (was mongodb)..."
  airflow connections delete mongodb_default || true
fi
airflow connections add mongodb_default --conn-type mongo --conn-host localhost --conn-port 27017 || \
  airflow connections add mongodb_default --conn-uri "mongodb://localhost:27017" || true

# Try `airflow standalone` first (available in newer Airflow versions)
if airflow standalone --help >/dev/null 2>&1; then
  echo "Running 'airflow standalone' (port $PORT). Press Ctrl+C to stop."
  AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$PORT" airflow standalone
  exit 0
fi

echo "Initializing Airflow DB..."
airflow db init

echo "Creating admin user (username: admin, password: admin) if not exists..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

echo "Starting scheduler in background (logs -> $AIRFLOW_HOME/logs/scheduler.log)..."
nohup airflow scheduler > "$AIRFLOW_HOME/logs/scheduler.log" 2>&1 &
echo $! > "$AIRFLOW_HOME/scheduler.pid"
sleep 3

echo "Starting webserver on port $PORT (foreground)."
airflow webserver --port "$PORT"
