"""Import ROR data from Zenodo into MongoDB (renamed to ror_capture).

Download the latest *ror-data.json* from the specified Zenodo record and load
into MongoDB `ror` database, collection `ror_stage` by default.
"""

from __future__ import annotations

import ast
import gzip
import hashlib
import io
import json
import logging
import os
import re
import zipfile
from datetime import datetime
from typing import Any, cast

import requests
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)


def _find_latest_ror_file(files: list[dict[str, Any]]) -> dict[str, Any] | None:
    pattern = re.compile(r"ror-data\.json$", re.IGNORECASE)
    matched = [f for f in files if pattern.search(f.get("key", ""))]
    if matched:
        matched.sort(key=lambda f: f.get("updated", f.get("created", "")))
        return matched[-1]
    if files:
        return files[-1]
    return None


def _compute_file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_ror_to_path(record_id: int = 18260365) -> dict[str, Any]:
    api_url = f"https://zenodo.org/api/records/{record_id}"
    log.info("Fetching Zenodo record %s", record_id)

    # Use a requests Session with retries for transient HTTP errors
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    session.mount("https://", HTTPAdapter(max_retries=cast(Any, retries)))

    res = session.get(api_url, timeout=30)
    res.raise_for_status()
    record = res.json()
    files = record.get("files", [])
    file_entry = _find_latest_ror_file(files)
    if not file_entry:
        raise RuntimeError(f"No files found on Zenodo record {record_id}")

    links = file_entry.get("links", {})
    download_url = links.get("download") or links.get("self")
    if not download_url:
        raise RuntimeError(f"No download link for file {file_entry}")

    # defer actual download until we decide based on cache

    filename = file_entry.get("key", f"ror-{record_id}")

    airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
    dest_dir = os.path.join(airflow_home, "ror_import", str(record_id))
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)

    # decide whether to download: if file exists, compare checksum or size
    already_present = os.path.exists(dest_path)
    remote_checksum = file_entry.get("checksum")
    remote_size = file_entry.get("size")
    if already_present:
        try:
            if remote_checksum:
                # normalize checksum (may be prefixed like "sha256:<hex>")
                expected = remote_checksum.split(":")[-1]
                local_hash = _compute_file_sha256(dest_path)
                if local_hash == expected:
                    log.info(
                        "Cached ROR file matches remote (checksum); skipping download: %s",
                        dest_path,
                    )
                    return {"file_path": dest_path, "is_new": False}
            elif remote_size is not None:
                if os.path.getsize(dest_path) == int(remote_size):
                    log.info(
                        "Cached ROR file matches remote (size); skipping download: %s", dest_path
                    )
                    return {"file_path": dest_path, "is_new": False}
        except Exception:
            # on any check error, fall back to re-download
            log.warning("Failed to validate cached file; will re-download %s", dest_path)

    # perform download with retrying session
    log.info("Downloading %s", download_url)
    dl = session.get(download_url, timeout=120)
    dl.raise_for_status()

    with open(dest_path, "wb") as fh:
        fh.write(dl.content)

    log.info("Saved file to %s", dest_path)
    return {"file_path": dest_path, "is_new": True}


def _copy_db(client, src_db_name: str, dst_db_name: str) -> None:
    src_db = client[src_db_name]
    dst_db = client[dst_db_name]
    for coll_name in src_db.list_collection_names():
        src_coll = src_db[coll_name]
        dst_coll = dst_db[coll_name]
        batch = []
        for doc in src_coll.find({}):
            # remove _id to avoid duplicate key problems when copying
            if "_id" in doc:
                doc.pop("_id")
            batch.append(doc)
            if len(batch) >= 1000:
                dst_coll.insert_many(batch)
                batch.clear()
        if batch:
            dst_coll.insert_many(batch)


def load_ror_from_path(
    file_info: dict | list | str,
    mongo_conn_id: str = "mongodb_default",
    db_name: str | None = None,
    collection_name: str = "ror_stage",
) -> int:
    """file_info may be a dict returned by download_ror_to_path or a plain path string."""
    # Coerce templated XCom string representations back to dict when needed
    if isinstance(file_info, str):
        s = file_info.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            parsed = None
            try:
                parsed = ast.literal_eval(s)
            except Exception:
                parsed = None
            if parsed is None:
                try:
                    parsed = json.loads(s)
                except Exception:
                    parsed = None
            if isinstance(parsed, dict | list):
                file_info = parsed

    if isinstance(file_info, dict):
        file_path = file_info.get("file_path")
        is_new = bool(file_info.get("is_new"))
    else:
        file_path = file_info
        is_new = True

    if not file_path or str(file_path).lower() == "none":
        airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
        base = os.path.join(airflow_home, "ror_import")
        latest = None
        latest_mtime = 0.0
        for root, _, files in os.walk(base if os.path.isdir(base) else "."):
            for fn in files:
                p = os.path.join(root, fn)
                try:
                    m = os.path.getmtime(p)
                except Exception:
                    m = 0
                if m > latest_mtime:
                    latest_mtime = m
                    latest = p
        if not latest:
            raise RuntimeError(f"No downloaded ROR file found in {base}")
        file_path = latest

    filename = os.path.basename(file_path)
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zf:
            candidates = [
                n for n in zf.namelist() if re.search(r"ror-data\.json$", n, re.IGNORECASE)
            ]
            if not candidates:
                candidates = [n for n in zf.namelist() if n.lower().endswith(".json")]
            if not candidates:
                raise RuntimeError(f"No JSON file found inside zip {file_path}")
            member = candidates[-1]
            with zf.open(member) as fh:
                data = json.load(io.TextIOWrapper(fh, encoding="utf-8"))
    elif filename.lower().endswith(".gz"):
        with gzip.open(file_path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        with open(file_path, encoding="utf-8") as fh:
            data = json.load(fh)

    # if file not new, skip loading
    if not is_new:
        log.info("No new ROR file; skipping DB load for %s", file_path)
        return 0

    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    conn = getattr(hook, "connection", None)
    if db_name is None:
        db_name = getattr(conn, "schema", None)
        if not db_name:
            raise ValueError(
                "MongoDB database not provided. Set `db_name` in the DAG params or provide it in the connection schema."
            )
    # move existing DB to new name with year and month
    now = datetime.utcnow()
    suffix = now.strftime("%Y%m")
    archived_db_name = f"{db_name}_{suffix}"
    # if source DB exists and has collections, copy then drop
    if db_name in client.list_database_names():
        src_db = client[db_name]
        if src_db.list_collection_names():
            log.info("Archiving existing DB %s -> %s", db_name, archived_db_name)
            _copy_db(client, db_name, archived_db_name)
            client.drop_database(db_name)

    db = client[db_name]
    coll = db[collection_name]

    if isinstance(data, dict):
        docs = data["items"] if "items" in data and isinstance(data["items"], list) else [data]
    elif isinstance(data, list):
        docs = data
    else:
        raise RuntimeError(f"Unexpected JSON structure in file {file_path}")

    if not docs:
        log.info("No documents to insert")
        return 0

    inserted = 0
    if all(isinstance(d, dict) and d.get("id") for d in docs):
        for doc in docs:
            doc_id = doc.get("id")
            coll.replace_one({"id": doc_id}, doc, upsert=True)
            inserted += 1
    else:
        coll.delete_many({})
        coll.insert_many(docs)
        inserted = len(docs)

    log.info("Inserted/updated %d documents into %s.%s", inserted, db_name, collection_name)
    return inserted


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": 300,
}


with DAG(
    dag_id="ror_capture",
    default_args=default_args,
    description="Download latest ROR JSON from Zenodo and load into MongoDB",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ror", "import"],
    params={"mongo_conn_id": "mongodb_default", "mongo_db": "ror"},
    is_paused_upon_creation=True,
) as dag:
    download_task = PythonOperator(
        task_id="download_ror",
        python_callable=download_ror_to_path,
        op_kwargs={"record_id": 18260365},
        do_xcom_push=True,
    )

    load_task = PythonOperator(
        task_id="load_ror",
        python_callable=load_ror_from_path,
        op_kwargs={
            "file_info": "{{ ti.xcom_pull(task_ids='download_ror') }}",
            "mongo_conn_id": "{{ params.mongo_conn_id }}",
            "db_name": "{{ params.mongo_db }}",
            "collection_name": "ror_stage",
        },
    )

    download_task >> load_task
