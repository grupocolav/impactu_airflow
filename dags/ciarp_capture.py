from __future__ import annotations

import contextlib
import io
import os
import pickle
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pymongo import ReplaceOne

from extract.base_extractor import BaseExtractor

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
XLS_MIME = "application/vnd.ms-excel"
FOLDER_MIME = "application/vnd.google-apps.folder"

CIARP_PREFIX = "ciarp_"
CIARP_DATE_FORMATS = ("%d_%m_%Y_%H:%M", "%Y-%m-%d_%H:%M")
CIARP_NAME_RE = re.compile(
    r"^ciarp_(?P<rorid>[^_]+)_(?P<date>\d{2}_\d{2}_\d{4}_\d{2}:\d{2}|\d{4}-\d{2}-\d{2}_\d{2}:\d{2})(?:_.*)?$",
    re.IGNORECASE,
)
UTC = getattr(datetime, "UTC", timezone(timedelta(0)))


class CiarpExtractor(BaseExtractor):
    """
    Extracts raw CIARP matrices from Google Drive and loads them into MongoDB.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI.
    db_name : str
        Database name.
    collection_name : str, optional
        Collection name (default: "ciarp").
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance.
    drive_root_folder_id : str
        Google Drive folder ID where institution subfolders live.
    google_token_pickle : str
        Path to the pickle containing Google Drive API credentials.
    cache_dir : str, optional
        Local directory used to cache downloads (default: "/opt/airflow/cache/ciarp").
    keep_only_latest_per_institution : bool, optional
        If True, deletes prior docs for the institution before loading the latest Excel (default: True).
    backup_existing : bool, optional
        If True, creates a timestamped backup collection before loading (default: True).
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        drive_root_folder_id: str,
        google_token_pickle: str,
        collection_name: str = "ciarp",
        client: Any = None,
        cache_dir: str | None = None,
        keep_only_latest_per_institution: bool = True,
        backup_existing: bool = True,
        subfolder_name: str | None = None,
    ):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)

        self.drive_root_folder_id = drive_root_folder_id
        self.google_token_pickle = google_token_pickle
        self.keep_only_latest_per_institution = keep_only_latest_per_institution
        self.backup_existing = backup_existing

        if cache_dir is None:
            airflow_home = os.environ.get("AIRFLOW_HOME")
            if airflow_home:
                cache_dir = str(Path(airflow_home) / "cache" / "ciarp")
            else:
                cache_dir = str(Path.home() / ".cache" / "impactu_airflow" / "ciarp")

        self.cache_dir = cache_dir
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

        self._drive_service = None
        if subfolder_name:
            self.drive_root_folder_id = self._resolve_subfolder_id(
                drive_root_folder_id, subfolder_name
            )
        self.create_indexes()

    # Mongo
    def create_indexes(self) -> None:
        """
        Ensure minimal indexes to support faster queries and upserts.
        """
        self.logger.info("Ensuring indexes for ciarp collection...")
        self.collection.create_index("institution_id")
        self.collection.create_index("drive_file_id")
        self.collection.create_index([("institution_id", 1), ("drive_file_id", 1)])
        self.collection.create_index("ciarp_file_date")
        self.collection.create_index("extracted_at")

    def _backup_collection_if_needed(self) -> None:
        """
        Create a timestamped backup collection if the target collection has data.
        """
        if not self.backup_existing:
            return

        try:
            existing_count = self.collection.estimated_document_count()
        except Exception:
            existing_count = self.collection.count_documents({})

        if existing_count == 0:
            return

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_name = f"{self.collection_name}_backup_{timestamp}"
        self.logger.info(
            f"Backing up {self.collection_name} -> {backup_name} ({existing_count} docs)..."
        )
        try:
            self.collection.aggregate(
                [
                    {"$match": {}},
                    {
                        "$merge": {
                            "into": backup_name,
                            "whenMatched": "replace",
                            "whenNotMatched": "insert",
                        }
                    },
                ],
                allowDiskUse=True,
            )
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")

    # Google Drive helpers
    def _get_drive_service(self):
        """
        Build a Google Drive v3 service from credentials stored in a pickle.
        """
        if self._drive_service is not None:
            return self._drive_service

        with open(self.google_token_pickle, "rb") as f:
            creds = pickle.load(f)

        if hasattr(creds, "expired") and creds.expired and getattr(creds, "refresh_token", None):
            creds.refresh(Request())

        self._drive_service = build("drive", "v3", credentials=creds)
        return self._drive_service

    def _list_files(
        self,
        q: str,
        fields: str = "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Generic Drive list() helper with pagination.
        """
        service = self._get_drive_service()
        out: list[dict[str, Any]] = []
        page_token = None

        while True:
            resp = (
                service.files()
                .list(
                    q=q,
                    pageSize=page_size,
                    fields=fields,
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    spaces="drive",
                )
                .execute()
            )

            out.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return out

    def _list_institution_folders(self) -> list[dict[str, Any]]:
        """
        List all first-level institution folders under the root folder.
        """
        q = (
            f"'{self.drive_root_folder_id}' in parents "
            f"and mimeType='{FOLDER_MIME}' and trashed=false"
        )
        return self._list_files(q=q)

    def _parse_institution_folder_name(self, folder_name: str) -> tuple[str | None, str]:
        """
        Expected folder naming format: RORID_INSTITUTION_NAME

        Returns
        -------
        (ror_id, institution_name)
            ror_id can be None if the format is invalid.
        """
        if "_" not in folder_name:
            return None, folder_name

        ror_id, rest = folder_name.split("_", 1)
        return ror_id.strip(), rest.strip()

    def _list_ciarp_excels(self, folder_id: str) -> list[dict[str, Any]]:
        """
        List Excel files (xlsx/xls) in a given institution folder.
        """
        q = (
            f"'{folder_id}' in parents and trashed=false and ("
            f"mimeType='{XLSX_MIME}' or mimeType='{XLS_MIME}'"
            f")"
        )
        files = self._list_files(q=q)
        return [f for f in files if (f.get("name") or "").lower().startswith(CIARP_PREFIX)]

    def _resolve_subfolder_id(self, parent_folder_id: str, subfolder_name: str) -> str:
        """
        Resolve a subfolder name to its Drive folder ID under the given parent.
        """
        q = f"'{parent_folder_id}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
        folders = self._list_files(q=q)
        target = subfolder_name.strip().lower()
        for folder in folders:
            name = (folder.get("name") or "").strip().lower()
            if name == target:
                return str(folder["id"])

        raise ValueError(f"Subfolder '{subfolder_name}' not found under drive_root_folder_id.")

    @staticmethod
    def _parse_ciarp_filename(
        file_name: str,
    ) -> tuple[str | None, datetime | None, str | None]:
        """
        Parse file name: ciarp_RORID_DD_MM_YYYY_HH:MM or ciarp_RORID_YYYY-MM-DD_HH:MM
        """
        base = Path(file_name).stem
        match = CIARP_NAME_RE.match(base)
        if not match:
            return None, None, None

        ror_id = match.group("rorid").strip()
        date_raw = match.group("date").strip()
        parsed_dt = None
        for fmt in CIARP_DATE_FORMATS:
            try:
                parsed_dt = datetime.strptime(date_raw, fmt)
                break
            except ValueError:
                continue

        if parsed_dt is None:
            return ror_id, None, date_raw

        return ror_id, parsed_dt, date_raw

    @staticmethod
    def _pick_latest_by_modified_time(
        files: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        Pick the file with the latest modifiedTime (Drive metadata).
        """
        if not files:
            return None

        def key(f: dict[str, Any]) -> str:
            return f.get("modifiedTime") or ""

        return max(files, key=key)

    def _download_file_to_cache(self, file_id: str, file_name: str) -> str:
        """
        Download a Drive file into the local cache (overwrites if it exists).
        """
        service = self._get_drive_service()
        safe_name = re.sub(r"[^\w\-.]+", "_", file_name)
        local_path = os.path.join(self.cache_dir, f"{file_id}__{safe_name}")

        request = service.files().get_media(fileId=file_id)

        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        return local_path

    # Excel -> Mongo
    def _read_excel_as_records(self, local_path: str) -> list[dict[str, Any]]:
        """
        Read an Excel file into a list of row dicts (raw, no normalization).
        """
        df = pd.read_excel(local_path, dtype=str)
        df = df.where(pd.notnull(df), None)
        records = cast(list[dict[str, Any]], df.to_dict(orient="records"))
        return records

    def _already_loaded(
        self, institution_id: str, drive_file_id: str, drive_modified_time: str
    ) -> bool:
        """
        Avoid reprocessing if we already loaded at least one doc for that fileId+modifiedTime.
        """
        return (
            self.collection.find_one(
                {
                    "institution_id": institution_id,
                    "drive_file_id": drive_file_id,
                    "drive_modified_time": drive_modified_time,
                },
                {"_id": 1},
            )
            is not None
        )

    def _replace_institution_data(
        self,
        institution_id: str,
        institution_name: str,
        folder_id: str,
        file_meta: dict[str, Any],
        records: list[dict[str, Any]],
        file_date: datetime | None,
        file_date_raw: str | None,
        chunk_size: int = 2000,
    ) -> None:
        """
        Delete and reload the institution's CIARP data (raw extract only).
        """
        drive_file_id = file_meta["id"]
        drive_file_name = file_meta.get("name", "")
        drive_modified_time = file_meta.get("modifiedTime", "")
        drive_size = file_meta.get("size")

        if self.keep_only_latest_per_institution:
            self.logger.info(f"Deleting previous ciarp docs for institution {institution_id}...")
            self.collection.delete_many({"institution_id": institution_id})

        extracted_at = datetime.now(UTC).isoformat()
        file_date_iso = file_date.isoformat() if file_date else None

        ops: list[ReplaceOne] = []
        for i, row in enumerate(records):
            doc = dict(row)
            doc["_id"] = f"{institution_id}:{drive_file_id}:{i}"
            doc["institution_id"] = institution_id
            doc["institution_name"] = institution_name
            doc["drive_folder_id"] = folder_id
            doc["drive_file_id"] = drive_file_id
            doc["drive_file_name"] = drive_file_name
            doc["drive_modified_time"] = drive_modified_time
            doc["drive_file_size"] = drive_size
            doc["ciarp_file_date"] = file_date_iso
            doc["ciarp_file_date_raw"] = file_date_raw
            doc["row_number"] = i
            doc["extracted_at"] = extracted_at

            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))

        total = len(ops)
        if total == 0:
            self.logger.warning(f"No rows found in {drive_file_name} for {institution_id}.")
            return

        self.logger.info(f"Writing {total} ciarp docs for {institution_id} (bulk upsert)...")
        for start in range(0, total, chunk_size):
            chunk = ops[start : start + chunk_size]
            self.collection.bulk_write(chunk, ordered=False)

    # Public API
    def process_all_files(self, force: bool = False) -> dict[str, Any]:
        """
        Process all CIARP files inside institution subfolders.

        Parameters
        ----------
        force : bool
            If True, reprocess even if the latest file was already loaded.
        """
        folders = self._list_institution_folders()
        self.logger.info(f"Found {len(folders)} institution folders under root.")

        stats = {"folders": len(folders), "processed": 0, "skipped": 0, "empty": 0, "errors": 0}

        if folders:
            self._backup_collection_if_needed()

        for folder in folders:
            folder_id = folder["id"]
            folder_name = folder.get("name", "")
            ror_id, inst_name = self._parse_institution_folder_name(folder_name)

            if not ror_id:
                self.logger.warning(f"Skipping folder with unexpected name format: {folder_name}")
                stats["skipped"] += 1
                continue

            try:
                excel_files = self._list_ciarp_excels(folder_id)
                if not excel_files:
                    self.logger.warning(f"No CIARP excel files found in {folder_name}")
                    stats["empty"] += 1
                    continue

                latest = self._pick_latest_by_modified_time(excel_files)
                if not latest:
                    stats["empty"] += 1
                    continue

                drive_file_id = latest["id"]
                drive_modified_time = latest.get("modifiedTime", "")

                if not force and self._already_loaded(ror_id, drive_file_id, drive_modified_time):
                    self.logger.info(
                        f"[SKIP] {ror_id} latest file already loaded: {latest.get('name')}"
                    )
                    stats["skipped"] += 1
                    continue

                file_name = latest.get("name", "")
                _, file_date, file_date_raw = self._parse_ciarp_filename(file_name)

                self.logger.info(
                    f"[{ror_id}] Latest CIARP file: {file_name} ({drive_modified_time})"
                )

                local_path = self._download_file_to_cache(
                    drive_file_id, latest.get("name", "ciarp.xlsx")
                )
                records = self._read_excel_as_records(local_path)

                self._replace_institution_data(
                    institution_id=ror_id,
                    institution_name=inst_name,
                    folder_id=folder_id,
                    file_meta=latest,
                    records=records,
                    file_date=file_date,
                    file_date_raw=file_date_raw,
                )

                stats["processed"] += 1
                with contextlib.suppress(Exception):
                    self.save_checkpoint(
                        f"ciarp_{ror_id}",
                        {
                            "drive_file_id": drive_file_id,
                            "drive_modified_time": drive_modified_time,
                        },
                    )

                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Error processing folder {folder_name}: {e}")
                stats["errors"] += 1

        return stats

    def run(self, force: bool = False) -> None:  # type: ignore[override]
        """
        Entry point, consistent with other extractors.
        """
        self.process_all_files(force=force)


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


def _coerce_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "t"):
            return True
        if lowered in ("false", "0", "no", "n", "f"):
            return False
    return bool(value)


def run_ciarp_capture(**kwargs: Any) -> None:
    """
    Capture CIARP files from Google Drive and load into MongoDB.

    Notes
    -----
    Uses MongoHook to get MongoDB connection from Airflow connections.
    Google Drive credentials are expected to be available as a pickle file path.
    """
    params = kwargs.get("params", {})
    dag_run = kwargs.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None):
        for key, value in dag_run.conf.items():
            if value not in (None, ""):
                params[key] = value

    drive_root_folder_id = params.get("drive_root_folder_id") or Variable.get(
        "ciarp_drive_root_folder_id", default_var=""
    )
    drive_subfolder_name = params.get("drive_subfolder_name") or Variable.get(
        "ciarp_drive_subfolder_name", default_var="ciarp"
    )
    dump_dir = params.get("dump_dir") or Variable.get("ciarp_dump_dir", default_var="")
    google_token_pickle = params.get("google_token_pickle")
    cache_dir = params.get("cache_dir", "/tmp/impactu_airflow_cache/ciarp")

    force = _coerce_bool(params.get("force", False))
    keep_only_latest_per_institution = _coerce_bool(
        params.get("keep_only_latest_per_institution", True)
    )
    backup_existing = _coerce_bool(params.get("backup_existing", True))

    if not drive_root_folder_id:
        raise ValueError("Missing required param: drive_root_folder_id")
    if not google_token_pickle:
        raise ValueError("Missing required param: google_token_pickle")

    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()
    db_name = hook.connection.schema or "institutional"

    extractor = CiarpExtractor(
        mongodb_uri="",
        db_name=db_name,
        drive_root_folder_id=drive_root_folder_id,
        subfolder_name=drive_subfolder_name or None,
        google_token_pickle=google_token_pickle,
        collection_name="ciarp",
        client=client,
        cache_dir=cache_dir,
        keep_only_latest_per_institution=keep_only_latest_per_institution,
        backup_existing=backup_existing,
    )

    try:
        extractor.dump_collection_if_exists(dump_dir, filename_prefix="ciarp")
        stats = extractor.process_all_files(force=force)
        print(f"CIARP capture finished. Stats: {stats}")
    finally:
        extractor.close()


with DAG(
    "ciarp_capture",
    default_args=default_args,
    description="Capture CIARP files from Google Drive and load into MongoDB",
    schedule="0 3 * * 1",
    catchup=False,
    tags=["capture", "ciarp"],
    params={
        "drive_root_folder_id": Param(
            "",
            type="string",
            description="Google Drive folder ID containing CIARP files",
        ),
        "drive_subfolder_name": Param(
            "ciarp",
            type="string",
            description="Optional subfolder name under drive_root_folder_id (e.g., Ciarp)",
        ),
        "dump_dir": Param(
            "",
            type="string",
            description="Optional directory for dumping the ciarp collection before load",
        ),
        "google_token_pickle": Param(
            "",
            type="string",
            description="Path to Google Drive credentials pickle file (read-only access)",
        ),
        "cache_dir": Param(
            "/tmp/impactu_airflow_cache/ciarp",
            type="string",
            description="Local cache directory for downloaded CIARP files",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Reprocess institutions even if the latest file was already loaded",
        ),
        "keep_only_latest_per_institution": Param(
            True,
            type="boolean",
            description="Delete previous docs for each institution before loading the latest file",
        ),
        "backup_existing": Param(
            True,
            type="boolean",
            description="Create a backup collection before loading if data exists",
        ),
    },
) as dag:
    extract_task = PythonOperator(
        task_id="ciarp_capture",
        python_callable=run_ciarp_capture,
    )
