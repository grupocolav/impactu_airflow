#!/usr/bin/env bash
# install_kahi_plugins.sh — Install Kahi core and all Kahi plugins from local submodules.
#
# Usage:
#   ./scripts/install_kahi_plugins.sh            # install everything
#   ./scripts/install_kahi_plugins.sh --dev       # install in editable (dev) mode
#   ./scripts/install_kahi_plugins.sh --plugin Kahi_doaj_sources  # install only one plugin
#
# Requires: pip (from the active virtualenv / conda env)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAHI_DIR="$REPO_ROOT/deps/kahi"
PLUGINS_DIR="$REPO_ROOT/deps/kahi_plugins"

EDITABLE=false
SINGLE_PLUGIN=""

# ── Parse arguments ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dev|-e)
            EDITABLE=true
            shift
            ;;
        --plugin|-p)
            SINGLE_PLUGIN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--dev] [--plugin PLUGIN_NAME]"
            echo ""
            echo "  --dev, -e         Install in editable mode (pip install -e)"
            echo "  --plugin, -p NAME Install only the specified plugin (e.g. Kahi_doaj_sources)"
            echo "  -h, --help        Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# ── Helpers ────────────────────────────────────────────────────────────────
install_package() {
    local pkg_dir="$1"
    local name
    name="$(basename "$pkg_dir")"

    if [[ ! -f "$pkg_dir/setup.py" && ! -f "$pkg_dir/pyproject.toml" ]]; then
        echo "  ⚠  Skipping $name — no setup.py or pyproject.toml found"
        return 0
    fi

    if $EDITABLE; then
        echo "  📦 Installing $name (editable)..."
        pip install -e "$pkg_dir" --quiet
    else
        echo "  📦 Installing $name..."
        pip install "$pkg_dir" --quiet
    fi
}

FAILED=()
INSTALLED=()

install_or_track() {
    local pkg_dir="$1"
    if install_package "$pkg_dir"; then
        INSTALLED+=("$(basename "$pkg_dir")")
    else
        FAILED+=("$(basename "$pkg_dir")")
    fi
}

# ── Sanity checks ─────────────────────────────────────────────────────────
if ! command -v pip &>/dev/null; then
    echo "ERROR: pip not found. Activate a virtualenv or conda env first." >&2
    exit 1
fi

if [[ ! -d "$KAHI_DIR" ]]; then
    echo "ERROR: Kahi core not found at $KAHI_DIR" >&2
    echo "  Run: git submodule update --init --recursive" >&2
    exit 1
fi

if [[ ! -d "$PLUGINS_DIR" ]]; then
    echo "ERROR: Kahi plugins not found at $PLUGINS_DIR" >&2
    echo "  Run: git submodule update --init --recursive" >&2
    exit 1
fi

echo "═══════════════════════════════════════════════════════════"
echo "  Kahi Plugins Installer"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── 1. Install Kahi core first (required dependency) ──────────────────────
echo "── Step 1: Installing Kahi core ──"
install_or_track "$KAHI_DIR"
echo ""

# ── 2. Install plugins ────────────────────────────────────────────────────
if [[ -n "$SINGLE_PLUGIN" ]]; then
    echo "── Step 2: Installing single plugin: $SINGLE_PLUGIN ──"
    target="$PLUGINS_DIR/$SINGLE_PLUGIN"
    if [[ ! -d "$target" ]]; then
        echo "ERROR: Plugin directory not found: $target" >&2
        echo "Available plugins:"
        ls -d "$PLUGINS_DIR"/Kahi_*/ 2>/dev/null | sed 's|.*/\(.*\)/|  \1|'
        exit 1
    fi
    install_or_track "$target"
else
    echo "── Step 2: Installing all plugins ──"
    for plugin_dir in "$PLUGINS_DIR"/Kahi_*/; do
        plugin_name="$(basename "$plugin_dir")"
        # Skip the template plugin
        if [[ "$plugin_name" == "Kahi_template" ]]; then
            echo "  ⏭  Skipping $plugin_name (template)"
            continue
        fi
        install_or_track "$plugin_dir"
    done
fi

echo ""

# ── Summary ────────────────────────────────────────────────────────────────
echo "═══════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════"
echo "  ✔ Installed: ${#INSTALLED[@]}"
if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo "  ✘ Failed:    ${#FAILED[@]}"
    for f in "${FAILED[@]}"; do
        echo "      - $f"
    done
    echo ""
    echo "  Re-run with verbose pip to debug:"
    echo "    pip install <plugin_dir>"
    exit 1
else
    echo "  ✘ Failed:    0"
fi
echo ""
echo "  Done! All Kahi plugins are ready."
