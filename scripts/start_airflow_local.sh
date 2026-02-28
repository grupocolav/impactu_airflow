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

# ── Load .env if present ───────────────────────────────────────────────────
if [[ -f "$REPO_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO_ROOT/.env"
    set +a
fi

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

# ── Ensure git submodules are initialised ──────────────────────────────────
if [[ ! -d "$KAHI_DIR/.git" ]] || [[ ! -d "$PLUGINS_DIR/.git" ]]; then
    echo "── Initialising git submodules ──"
    git -C "$REPO_ROOT" submodule update --init --recursive
fi

if ! $SKIP_INSTALL; then
    echo "═══════════════════════════════════════════════════════════"
    echo "  Setting up environment with uv"
    echo "═══════════════════════════════════════════════════════════"
    echo ""

    # ── 1. Create venv ─────────────────────────────────────────────────────
    if [[ ! -d "$VENV_DIR" ]]; then
        echo "── Creating virtualenv (.venv) with Python $PYTHON_VERSION ──"
        uv venv "$VENV_DIR" --python "$PYTHON_VERSION"
    else
        echo "── Reusing existing virtualenv at $VENV_DIR ──"
    fi

    # ── Verify Python version is <= 3.12 ──────────────────────────────────
    VENV_PY_VERSION=$("$VENV_DIR/bin/python" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    VENV_PY_MINOR=$("$VENV_DIR/bin/python" -c "import sys; print(sys.version_info.minor)")
    if [[ "$VENV_PY_MINOR" -gt 12 ]]; then
        echo ""
        echo "⚠  WARNING: Python $VENV_PY_VERSION detected in the virtualenv." >&2
        echo "   This project requires Python <= 3.12 due to native dependencies" >&2
        echo "   (fasttext, hunspell) that fail to build on newer versions." >&2
        echo "   Please install Python 3.12 and re-run, or set PYTHON_VERSION." >&2
        echo "   Example: uv python install 3.12" >&2
        exit 1
    fi

    # ── 2. Install project + requirements ──────────────────────────────────
    echo ""
    echo "── Installing build dependencies (pybind11) ──"
    uv pip install --python "$VENV_DIR/bin/python" pybind11 setuptools wheel

    echo ""
    echo "── Installing project dependencies ──"
    uv pip install --python "$VENV_DIR/bin/python" \
        apache-airflow \
        -r "$REPO_ROOT/requirements.txt"

    echo ""
    echo "── Installing project in editable mode ──"
    uv pip install --python "$VENV_DIR/bin/python" -e "$REPO_ROOT"

    # ── 3. Install kahi_impactu_utils (dependency of kahi and plugins) ─────
    #   fasttext==0.9.2 (dep of fastspell → kahi_impactu_utils) has two issues:
    #     a) Doesn't declare pybind11 as a build dep → needs --no-build-isolation
    #     b) Missing #include <cstdint> → fails with GCC 13+ / modern compilers
    #   hunspell==0.5.5 needs libhunspell-dev headers/libs from the system.
    #   The conda toolchain's linker doesn't find system libs in uv's build
    #   isolation, so we pre-build both with --no-build-isolation.
    export LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:${LIBRARY_PATH:-}"
    export CPATH="/usr/include:${CPATH:-}"

    echo ""
    echo "── Installing fasttext==0.9.2 (needs pybind11 + cstdint fix) ──"
    CXXFLAGS="-include cstdint" \
        uv pip install --python "$VENV_DIR/bin/python" \
        fasttext==0.9.2 --no-build-isolation

    echo ""
    echo "── Installing hunspell==0.5.5 (needs libhunspell-dev) ──"
    uv pip install --python "$VENV_DIR/bin/python" \
        hunspell==0.5.5 --no-build-isolation

    echo ""
    echo "── Installing kahi_impactu_utils (deps/kahi_impactu_utils) ──"
    uv pip install --python "$VENV_DIR/bin/python" -e "$REPO_ROOT/deps/kahi_impactu_utils"

    # ── 4. Install Kahi core ───────────────────────────────────────────────
    echo ""
    echo "── Installing Kahi core (deps/kahi) ──"
    uv pip install --python "$VENV_DIR/bin/python" -e "$KAHI_DIR"

    # ── 5. Install all Kahi plugins ────────────────────────────────────────
    echo ""
    echo "── Installing Kahi plugins ──"
    bash "$REPO_ROOT/scripts/install_kahi_plugins.sh" --python "$VENV_DIR/bin/python"

    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Installation complete"
    echo "═══════════════════════════════════════════════════════════"
fi

# ── Activate the venv so all airflow commands use the right Python ─────────
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# Ensure a local MongoDB connection exists for DAGs that expect `mongodb_default`.
# This is safe to run multiple times; the command will succeed or be ignored.
if airflow connections get mongodb_default -o json 2>/dev/null | grep -q '"conn_type": "mongodb"'; then
  echo "Recreating mongodb_default with conn_type=mongo (was mongodb)..."
  airflow connections delete mongodb_default || true
fi
airflow connections add mongodb_default --conn-type mongo --conn-host localhost --conn-port 27017 || \
  airflow connections add mongodb_default --conn-uri "mongodb://localhost:27017" || true

# ── Set Airflow Variables from .env ────────────────────────────────────────
if [[ -n "${GOOGLE_TOKEN_PICKLE:-}" ]]; then
    airflow variables set google_token_pickle "$GOOGLE_TOKEN_PICKLE" || true
fi

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
