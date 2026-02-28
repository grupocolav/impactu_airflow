"""Kahi processing DAG with per-plugin backup and restore protection.

Reads ``config/kahi_impactu.yml`` and creates a sequential Airflow task for
every plugin listed in the ``workflow`` section.  Before each plugin executes,
a full ``mongodump`` of the target database is taken.  If the plugin fails the
database is automatically restored from that backup so the next plugin starts
from a known-good state.

This prevents data corruption by ensuring each plugin either completes
successfully **or** leaves the database in its pre-execution state.

Requirements
------------
* ``mongodump`` and ``mongorestore`` must be available on ``$PATH``.
* ``deps/kahi`` and ``deps/kahi_plugins`` submodules must be present.
* Every Kahi plugin referenced in the YAML must be importable — either
  pip-installed or reachable via the ``deps/kahi_plugins/Kahi_*`` directories
  (added to ``sys.path`` automatically by this DAG).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any

import yaml  # type: ignore[import-untyped]
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# YAML ordered loader — preserves plugin execution order (same as Kahi core)
# ---------------------------------------------------------------------------


class _OrderedLoader(yaml.SafeLoader):
    """YAML loader that keeps mapping order (required by Kahi)."""


def _construct_ordered_mapping(loader, node):
    return OrderedDict(loader.construct_pairs(node))


_OrderedLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_ordered_mapping,
)


class _PlainDumper(yaml.SafeDumper):
    """YAML dumper that writes OrderedDicts as plain mappings."""


_PlainDumper.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_dict(data.items()),
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_WORKFLOW_PATH = os.path.join(REPO_ROOT, "config", "kahi_impactu.yml")
DEFAULT_BACKUP_DIR = os.path.join(REPO_ROOT, "backups")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_workflow(yaml_path: str) -> tuple[dict, OrderedDict]:
    """Return ``(config, workflow)`` from a Kahi YAML file."""
    with open(yaml_path) as fh:
        data = yaml.load(fh, Loader=_OrderedLoader)
    return data["config"], data["workflow"]


def _build_mongo_uri(config: dict) -> str:
    """Construct a ``mongodb://`` URI from the Kahi config block."""
    url: str = config.get("database_url", "localhost")
    if not url.startswith("mongodb"):
        url = f"mongodb://{url}" if ":" in url else f"mongodb://{url}:27017"
    return url


def _mongodump(uri: str, db_name: str, archive_path: str) -> None:
    """Run ``mongodump`` and raise on failure."""
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    cmd = [
        "mongodump",
        f"--uri={uri}",
        f"--db={db_name}",
        f"--archive={archive_path}",
        "--gzip",
    ]
    log.info("mongodump → %s", archive_path)
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if res.returncode != 0:
        raise RuntimeError(f"mongodump failed (rc={res.returncode}): {res.stderr}")
    log.info("Backup created: %s", archive_path)


def _mongorestore(uri: str, db_name: str, archive_path: str) -> None:
    """Run ``mongorestore --drop`` and raise on failure."""
    cmd = [
        "mongorestore",
        f"--uri={uri}",
        f"--archive={archive_path}",
        "--gzip",
        "--drop",
        f"--nsInclude={db_name}.*",
    ]
    log.info("mongorestore ← %s", archive_path)
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if res.returncode != 0:
        raise RuntimeError(f"mongorestore failed (rc={res.returncode}): {res.stderr}")
    log.info("Database '%s' restored from %s", db_name, archive_path)


def _setup_kahi_paths() -> None:
    """Add ``deps/kahi`` and every ``deps/kahi_plugins/Kahi_*`` to *sys.path*.

    This makes all Kahi plugins importable without requiring a ``pip install``
    for each one — useful when working directly from the git submodules.
    """
    kahi_dep = os.path.join(REPO_ROOT, "deps", "kahi")
    if kahi_dep not in sys.path:
        sys.path.insert(0, kahi_dep)

    plugins_root = os.path.join(REPO_ROOT, "deps", "kahi_plugins")
    if os.path.isdir(plugins_root):
        for entry in sorted(os.listdir(plugins_root)):
            full = os.path.join(plugins_root, entry)
            if os.path.isdir(full) and entry.startswith("Kahi_") and full not in sys.path:
                sys.path.insert(0, full)


# ---------------------------------------------------------------------------
# Main task callable
# ---------------------------------------------------------------------------


def run_plugin_with_backup(
    plugin_name: str,
    workflow_yaml_path: str,
    backup_dir: str,
    **kwargs: Any,
) -> None:
    """Execute a single Kahi plugin with backup / restore protection.

    1. ``mongodump`` the target database.
    2. Run the plugin via the ``Kahi`` orchestrator.
    3. On failure: ``mongorestore --drop`` from the backup, then re-raise so
       Airflow marks the task as **failed**.

    Because every downstream task uses ``trigger_rule="all_done"`` the pipeline
    continues to the next plugin even when the current one fails.
    """
    config, workflow = _parse_workflow(workflow_yaml_path)

    if plugin_name not in workflow:
        raise ValueError(
            f"Plugin '{plugin_name}' not found in workflow YAML " f"({workflow_yaml_path})"
        )

    db_name = config["database_name"]
    mongo_uri = _build_mongo_uri(config)

    safe = plugin_name.replace("/", "__")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive = os.path.join(backup_dir, f"{safe}_{ts}.archive.gz")

    # ── 1. Backup ──────────────────────────────────────────────────────────
    log.info("▶ Backing up '%s' before plugin '%s'", db_name, plugin_name)
    _mongodump(mongo_uri, db_name, archive)

    # ── 2. Run the plugin ──────────────────────────────────────────────────
    try:
        log.info("▶ Running plugin '%s'", plugin_name)

        # Build a single-plugin YAML that Kahi can load directly
        single_cfg = OrderedDict(
            [
                ("config", config),
                ("workflow", OrderedDict([(plugin_name, workflow[plugin_name])])),
            ]
        )

        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                suffix=".yml",
                mode="w",
                delete=False,
                prefix=f"kahi_{safe}_",
            ) as tmp:
                tmp_path = tmp.name
                yaml.dump(
                    single_cfg,
                    tmp,
                    Dumper=_PlainDumper,
                    default_flow_style=False,
                    allow_unicode=True,
                )

            _setup_kahi_paths()
            from kahi.Kahi import Kahi  # noqa: E402 — deferred import

            kahi = Kahi(tmp_path, verbose=4, use_log=True)
            kahi.run()
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        log.info("✔ Plugin '%s' completed successfully", plugin_name)

    except Exception:
        # ── 3. Restore on failure ──────────────────────────────────────────
        log.exception("✘ Plugin '%s' failed — restoring database from backup", plugin_name)
        try:
            _mongorestore(mongo_uri, db_name, archive)
            log.info("✔ Database restored after '%s' failure", plugin_name)
        except Exception:
            log.critical(
                "CRITICAL — could not restore DB after '%s' failure",
                plugin_name,
                exc_info=True,
            )
        # Re-raise so Airflow marks the task as FAILED
        raise


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

try:
    _cfg, _wf = _parse_workflow(DEFAULT_WORKFLOW_PATH)
    _plugins: list[str] = list(_wf.keys())
except Exception:
    log.exception("Could not parse %s — DAG will have no tasks", DEFAULT_WORKFLOW_PATH)
    _plugins = []

_default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(hours=24),
}

with DAG(
    dag_id="kahi_processing",
    default_args=_default_args,
    description=(
        "Run Kahi ETL plugins sequentially with per-step " "MongoDB backup/restore protection"
    ),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["processing", "kahi", "etl"],
    params={
        "workflow_yaml": DEFAULT_WORKFLOW_PATH,
        "backup_dir": DEFAULT_BACKUP_DIR,
    },
) as dag:
    prev_task = None

    for idx, plugin_name in enumerate(_plugins):
        safe_id = plugin_name.replace("/", "__")
        task = PythonOperator(
            task_id=f"{idx:03d}_{safe_id}",
            python_callable=run_plugin_with_backup,
            op_kwargs={
                "plugin_name": plugin_name,
                "workflow_yaml_path": "{{ params.workflow_yaml }}",
                "backup_dir": "{{ params.backup_dir }}",
            },
            # all_done → execute even if the previous plugin failed
            trigger_rule="all_done" if prev_task else "all_success",
        )
        if prev_task:
            prev_task >> task
        prev_task = task
