#!/usr/bin/env bash
# start_airflow_local.sh — Bootstrap a venv with uv, install deps + kahi plugins, and start Airflow.
#
# Usage:
#   ./scripts/start_airflow_local.sh                # full setup + start
#   ./scripts/start_airflow_local.sh --skip-install  # reuse existing venv, just start Airflow
#   AIRFLOW_PORT=9090 ./scripts/start_airflow_local.sh  # custom port
#
# Requires: uv (https://docs.astral.sh/uv/getting-started/installation/)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$REPO_ROOT/.venv"
KAHI_DIR="$REPO_ROOT/deps/kahi"
PLUGINS_DIR="$REPO_ROOT/deps/kahi_plugins"
PYTHON_VERSION="3.12"

AIRFLOW_HOME="${AIRFLOW_HOME:-$REPO_ROOT}"
export AIRFLOW_HOME
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
PORT="${AIRFLOW_PORT:-8080}"
# Airflow 3 workers call the Execution API. If UI/API port is customized,
# point workers to the same port to avoid queued->failed "state mismatch".
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="${AIRFLOW__CORE__EXECUTION_API_SERVER_URL:-http://localhost:${PORT}/execution/}"

SKIP_INSTALL=false

# ── Parse arguments ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-install|-s)
            SKIP_INSTALL=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--skip-install]"
            echo ""
            echo "  --skip-install, -s  Skip venv creation and package installation"
            echo "  -h, --help          Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ── Ensure uv is available ────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    echo "ERROR: uv not found. Install it first:" >&2
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
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

if airflow standalone --help >/dev/null 2>&1; then
    echo "Running 'airflow standalone'. Press Ctrl+C to stop."
    AIRFLOW__WEBSERVER__WEB_SERVER_PORT="$PORT" airflow standalone
    exit 0
fi

echo "Initializing Airflow DB..."
airflow db init

echo "Creating admin user (admin/admin) if not exists..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

echo "Starting scheduler in background..."
nohup airflow scheduler > "$AIRFLOW_HOME/logs/scheduler.log" 2>&1 &
echo $! > "$AIRFLOW_HOME/scheduler.pid"
sleep 3

echo "Starting webserver on port $PORT (foreground)."
airflow webserver --port "$PORT"
