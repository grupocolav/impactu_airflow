"""DOAJ data dump extractor."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tarfile
import time
from collections.abc import Generator, Iterable
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from pymongo import UpdateOne

from extract.base_extractor import BaseExtractor

UTC = getattr(datetime, "UTC", timezone(timedelta(0)))


class DoajExtractor(BaseExtractor):
    """
    Extractor for DOAJ public data dumps (journals and articles).

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI
    db_name : str
        Database name
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance
    cache_dir : str, optional
        Base cache directory for downloads and extracted dumps
    api_key : str, optional
        DOAJ API key (can be provided later in run/process methods)
    keep_cache : bool, optional
        Keep downloaded/extracted files (default: False)
    """

    BASE_URL = "https://doaj.org/public-data-dump"

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        client: Any = None,
        cache_dir: str | None = None,
        api_key: str | None = None,
        keep_cache: bool = False,
        journals_collection: str = "stage",
        articles_collection: str = "articles",
    ):
        super().__init__(mongodb_uri, db_name, collection_name=journals_collection, client=client)
        self.api_key = api_key
        self.keep_cache = keep_cache
        self.journals_collection = journals_collection
        self.articles_collection = articles_collection

        if cache_dir is None:
            cache_dir = "/tmp/impactu_airflow_cache/doaj"

        self.cache_dir = cache_dir
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(f"airflow.task.{self.__class__.__name__}")

        self._collections = {
            "journal": self.db[self.journals_collection],
            "article": self.db[self.articles_collection],
        }

    # ---------- Public API ----------
    def run(  # type: ignore[override]
        self,
        api_key: str | None = None,
        download_journals: bool = True,
        download_articles: bool = True,
        chunk_size: int = 1000,
        force_download: bool = False,
    ) -> dict[str, Any]:
        """
        Run DOAJ extraction for the selected dump types.
        """
        api_key = api_key or self.api_key or os.environ.get("DOAJ_API_KEY")
        if not api_key:
            raise ValueError("Missing DOAJ API key (api_key or DOAJ_API_KEY).")

        self.create_indexes()
        stats: dict[str, Any] = {
            "journals": None,
            "articles": None,
            "started_at": datetime.now(UTC).isoformat(),
        }

        if download_journals:
            stats["journals"] = self.process_dump(
                dump_type="journal",
                api_key=api_key,
                chunk_size=chunk_size,
                force_download=force_download,
            )

        if download_articles:
            stats["articles"] = self.process_dump(
                dump_type="article",
                api_key=api_key,
                chunk_size=chunk_size,
                force_download=force_download,
            )

        stats["finished_at"] = datetime.now(UTC).isoformat()
        return stats

    def process_dump(
        self,
        dump_type: str,
        api_key: str,
        chunk_size: int = 1000,
        force_download: bool = False,
    ) -> dict[str, Any]:
        """
        Download, extract, and load a DOAJ dump into MongoDB.
        """
        if dump_type not in {"journal", "article"}:
            raise ValueError("dump_type must be 'journal' or 'article'")

        start = time.monotonic()
        tar_path = self.download_dump(dump_type, api_key, force_download=force_download)
        extract_dir = self.extract_tar(tar_path)

        counts: dict[str, Any] = {
            "files": 0,
            "records": 0,
            "upserted": 0,
            "matched": 0,
            "modified": 0,
            "skipped": 0,
            "errors": 0,
        }

        try:
            for file_path in self.iterate_batches(extract_dir, dump_type=dump_type):
                counts["files"] += 1
                file_counts = self._process_file(
                    file_path, dump_type=dump_type, chunk_size=chunk_size
                )
                for key, value in file_counts.items():
                    counts[key] += value

                self.logger.info(
                    "Processed %s: records=%s upserted=%s matched=%s modified=%s skipped=%s errors=%s",
                    file_path,
                    file_counts["records"],
                    file_counts["upserted"],
                    file_counts["matched"],
                    file_counts["modified"],
                    file_counts["skipped"],
                    file_counts["errors"],
                )
        finally:
            if not self.keep_cache:
                self._cleanup_paths([tar_path, extract_dir])

        elapsed = time.monotonic() - start
        counts["elapsed_seconds"] = round(elapsed, 2)
        return counts

    # ---------- Download / Extract ----------
    def download_dump(self, dump_type: str, api_key: str, force_download: bool = False) -> str:
        """
        Download the DOAJ dump (.tar.gz) for the given type.
        """
        if dump_type not in {"journal", "article"}:
            raise ValueError("dump_type must be 'journal' or 'article'")

        filename = f"doaj_{dump_type}.tar.gz"
        target_path = os.path.join(self.cache_dir, filename)

        if not force_download and os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            self.logger.info("Using cached DOAJ %s dump: %s", dump_type, target_path)
            return target_path

        url = f"{self.BASE_URL}/{dump_type}"
        params = {"api_key": api_key}
        tmp_path = f"{target_path}.tmp"

        for attempt in range(1, 4):
            try:
                self.logger.info("Downloading DOAJ %s dump (attempt %s)...", dump_type, attempt)
                with requests.get(url, params=params, stream=True, timeout=120) as response:
                    response.raise_for_status()
                    with open(tmp_path, "wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)

                os.replace(tmp_path, target_path)
                self.logger.info("Downloaded DOAJ %s dump to %s", dump_type, target_path)
                return target_path
            except Exception as exc:
                self.logger.warning("Download failed (attempt %s): %s", attempt, exc)
                if os.path.exists(tmp_path):
                    with contextlib.suppress(OSError):
                        os.remove(tmp_path)

        raise RuntimeError(f"Failed to download DOAJ {dump_type} dump after retries")

    def extract_tar(self, tar_path: str) -> str:
        """
        Extract a .tar.gz file to a dedicated directory.
        """
        extract_dir = os.path.join(self.cache_dir, Path(tar_path).stem.replace(".tar", ""))
        Path(extract_dir).mkdir(parents=True, exist_ok=True)

        try:
            with tarfile.open(tar_path, "r:gz") as tar:
                tar.extractall(extract_dir)
        except tarfile.ReadError as exc:
            raise RuntimeError(f"Invalid tar file: {tar_path}") from exc

        return extract_dir

    def iterate_batches(self, directory: str, dump_type: str) -> Iterable[str]:
        """
        Yield JSON batch file paths for the given dump type.
        """
        prefix = "journal_batch_" if dump_type == "journal" else "article_batch_"
        for path in sorted(Path(directory).rglob("*.json")):
            if path.name.startswith(prefix):
                yield str(path)

    # ---------- Processing ----------
    def _process_file(self, file_path: str, dump_type: str, chunk_size: int) -> dict[str, int]:
        counts = {
            "records": 0,
            "upserted": 0,
            "matched": 0,
            "modified": 0,
            "skipped": 0,
            "errors": 0,
        }
        operations: list[UpdateOne] = []

        for record in self._iter_records(file_path):
            counts["records"] += 1
            op = self._build_upsert(record, dump_type)
            if op is None:
                counts["skipped"] += 1
                continue

            operations.append(op)
            if len(operations) >= chunk_size:
                self._flush_operations(operations, dump_type, counts)

        if operations:
            self._flush_operations(operations, dump_type, counts)

        return counts

    def _flush_operations(
        self,
        operations: list[UpdateOne],
        dump_type: str,
        counts: dict[str, int],
    ) -> None:
        collection = self._collections[dump_type]
        try:
            result = collection.bulk_write(operations, ordered=False)
        except Exception as exc:
            self.logger.error("Bulk write failed: %s", exc)
            counts["errors"] += len(operations)
        else:
            counts["upserted"] += len(result.upserted_ids)
            counts["matched"] += result.matched_count
            counts["modified"] += result.modified_count
        finally:
            operations.clear()

    def _build_upsert(self, record: dict[str, Any], dump_type: str) -> UpdateOne | None:
        derived = self._derive_fields(record, dump_type)
        source_id = derived.get("source_id")
        if not source_id:
            return None

        now = datetime.now(UTC).isoformat()
        doc = record.copy()
        doc.update(derived)
        doc["record_type"] = dump_type
        doc["updated_at"] = now

        update = {
            "$set": doc,
            "$setOnInsert": {"created_at": now},
        }

        return UpdateOne({"_id": source_id}, update, upsert=True)

    @staticmethod
    def _derive_fields(record: dict[str, Any], dump_type: str) -> dict[str, Any]:
        doaj_id = record.get("id") or record.get("_id")
        bibjson = record.get("bibjson") or {}

        doi = DoajExtractor._extract_doi(bibjson)
        pissn = bibjson.get("pissn")
        eissn = bibjson.get("eissn")
        issn_list = bibjson.get("issn") or []
        if isinstance(issn_list, str):
            issn_list = [issn_list]

        publisher = bibjson.get("publisher") or record.get("publisher")
        year = bibjson.get("year") or record.get("year")
        subjects = bibjson.get("subject") or bibjson.get("subjects") or []

        source_id = None
        if dump_type == "article":
            if doi:
                source_id = f"doi:{doi.lower()}"
            elif doaj_id:
                source_id = f"doaj:{doaj_id}"
        else:
            if doaj_id:
                source_id = f"doaj:{doaj_id}"
            elif pissn or eissn or issn_list:
                parts = [pissn, eissn] + list(issn_list)
                joined_parts: list[str] = [str(p) for p in parts if p not in (None, "")]
                source_id = f"issn:{'|'.join(joined_parts)}"

        if source_id is None:
            # Fallback to a stable hash if we truly have no identifier
            payload = json.dumps(record, sort_keys=True, default=str)
            source_id = f"hash:{sha1(payload.encode('utf-8')).hexdigest()}"

        return {
            "source_id": source_id,
            "doaj_id": doaj_id,
            "doi": doi,
            "pissn": pissn,
            "eissn": eissn,
            "issn": issn_list or None,
            "publisher": publisher,
            "year": year,
            "subjects": subjects,
        }

    @staticmethod
    def _extract_doi(bibjson: dict[str, Any]) -> str | None:
        identifiers = bibjson.get("identifier") or []
        if isinstance(identifiers, dict):
            identifiers = [identifiers]

        for identifier in identifiers:
            if not isinstance(identifier, dict):
                continue
            if (identifier.get("type") or "").lower() == "doi":
                for key in ("id", "value", "identifier"):
                    value = identifier.get(key)
                    if value:
                        return str(value).strip()
        return None

    @staticmethod
    def _iter_records(file_path: str) -> Generator[dict[str, Any], None, None]:
        """
        Yield records from a DOAJ batch file.

        Handles NDJSON, JSON arrays, and dicts with 'results'/'items' lists.
        """

        def _yield_from_loaded(data: Any) -> Generator[dict[str, Any], None, None]:
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        yield item
            elif isinstance(data, dict):
                for key in ("results", "items", "data", "records"):
                    value = data.get(key)
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                yield item
                        return
                yield data

        # Try NDJSON first
        with open(file_path, encoding="utf-8") as handle:
            first_line = ""
            while True:
                line = handle.readline()
                if not line:
                    break
                if line.strip():
                    first_line = line
                    break

            if first_line:
                try:
                    obj = json.loads(first_line)
                except json.JSONDecodeError:
                    pass
                else:
                    if isinstance(obj, dict):
                        # If this looks like a full JSON response, handle it directly
                        if any(key in obj for key in ("results", "items", "data", "records")):
                            yield from _yield_from_loaded(obj)
                            return

                        # Peek next non-empty line to decide NDJSON
                        next_line = ""
                        while True:
                            line = handle.readline()
                            if not line:
                                break
                            if line.strip():
                                next_line = line
                                break

                        if next_line:
                            try:
                                next_obj = json.loads(next_line)
                            except json.JSONDecodeError:
                                pass
                            else:
                                if isinstance(next_obj, dict):
                                    yield obj
                                    yield next_obj
                                    for line in handle:
                                        if line.strip():
                                            try:
                                                item = json.loads(line)
                                            except json.JSONDecodeError:
                                                break
                                            if isinstance(item, dict):
                                                yield item
                                    return

                        # Single JSON object (non-NDJSON)
                        yield from _yield_from_loaded(obj)
                        return

        # Fallback to full JSON load
        with open(file_path, encoding="utf-8") as handle:
            data = json.load(handle)
            yield from _yield_from_loaded(data)

    # ---------- Indexes / Cleanup ----------
    def create_indexes(self) -> None:
        """
        Create useful indexes for DOAJ collections.
        """
        journals = self._collections["journal"]
        articles = self._collections["article"]

        journals.create_index("doaj_id")
        journals.create_index("issn")
        journals.create_index("pissn")
        journals.create_index("eissn")
        journals.create_index("publisher")
        journals.create_index("subjects")

        articles.create_index("doaj_id")
        articles.create_index("doi")
        articles.create_index("year")
        articles.create_index("publisher")
        articles.create_index("subjects")

    def _cleanup_paths(self, paths: Iterable[str]) -> None:
        for path in paths:
            if not path:
                continue
            try:
                if os.path.isdir(path):
                    for child in Path(path).rglob("*"):
                        if child.is_file():
                            child.unlink()
                    for child in sorted(Path(path).rglob("*"), reverse=True):
                        if child.is_dir():
                            child.rmdir()
                    Path(path).rmdir()
                elif os.path.exists(path):
                    os.remove(path)
            except OSError:
                self.logger.warning("Failed to clean up %s", path)


def _compute_period(date_value: datetime) -> tuple[int, int]:
    month = date_value.month
    semester = 1 if month <= 6 else 2
    return date_value.year, semester


def run_doaj_capture(**kwargs: Any) -> None:
    params = kwargs.get("params", {})

    api_key = params.get("api_key") or Variable.get("doaj_api_key", default_var="")
    if not api_key:
        api_key = Variable.get("DOAJ_API_KEY", default_var="") or os.environ.get("DOAJ_API_KEY", "")
    if not api_key:
        raise ValueError("Missing DOAJ API key (Airflow Variable doaj_api_key or DOAJ_API_KEY)")

    download_journals = params.get("download_journals", True)
    download_articles = params.get("download_articles", False)
    chunk_size = int(params.get("chunk_size", 1000))
    force_download = params.get("force_download", False)

    cache_dir = params.get("cache_dir") or "/tmp/impactu_airflow_cache/doaj"
    keep_cache = params.get("keep_cache", False)
    journals_collection = params.get("journals_collection", "stage")
    articles_collection = params.get("articles_collection", "articles")

    period_year = params.get("period_year") or 0
    period_semester = params.get("period_semester") or 0

    if period_year > 0 and period_semester > 0:
        year = int(period_year)
        semester = int(period_semester)
    else:
        logical_date = (
            kwargs.get("data_interval_start")
            or kwargs.get("logical_date")
            or kwargs.get("execution_date")
            or datetime.now(UTC)
        )
        year, semester = _compute_period(logical_date)

    db_name = f"doaj_{year}_{semester}"

    hook = MongoHook(mongo_conn_id="mongodb_default")
    client = hook.get_conn()

    extractor = DoajExtractor(
        mongodb_uri="",
        db_name=db_name,
        client=client,
        cache_dir=cache_dir,
        api_key=api_key,
        keep_cache=keep_cache,
        journals_collection=journals_collection,
        articles_collection=articles_collection,
    )

    try:
        stats = extractor.run(
            api_key=api_key,
            download_journals=download_journals,
            download_articles=download_articles,
            chunk_size=chunk_size,
            force_download=force_download,
        )
        print(f"DOAJ capture finished. Stats: {stats}")
    finally:
        extractor.close()


default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


with DAG(
    "doaj_capture",
    default_args=default_args,
    description="Capture DOAJ public data dumps (journals and articles) into MongoDB",
    schedule=Variable.get("doaj_schedule", default_var="@monthly"),
    catchup=False,
    tags=["capture", "doaj"],
    params={
        "api_key": Param(
            "",
            type="string",
            description="DOAJ API key (overrides Airflow Variables)",
        ),
        "download_journals": Param(
            True,
            type="boolean",
            description="Download and load journal dump",
        ),
        "download_articles": Param(
            False,
            type="boolean",
            description="Download and load article dump",
        ),
        "chunk_size": Param(
            1000,
            type="integer",
            description="Bulk write chunk size",
        ),
        "force_download": Param(
            False,
            type="boolean",
            description="Force re-download even if cached",
        ),
        "cache_dir": Param(
            "/tmp/impactu_airflow_cache/doaj",
            type="string",
            description="Optional cache directory for downloads/extractions",
        ),
        "keep_cache": Param(
            False,
            type="boolean",
            description="Keep cache files after successful extraction",
        ),
        "journals_collection": Param(
            "stage",
            type="string",
            description="Mongo collection for DOAJ journals",
        ),
        "articles_collection": Param(
            "articles",
            type="string",
            description="Mongo collection for DOAJ articles",
        ),
        "period_year": Param(
            0,
            type="integer",
            description="Override DB year (e.g. 2025)",
        ),
        "period_semester": Param(
            0,
            type="integer",
            description="Override DB semester (1 or 2)",
        ),
    },
) as dag:
    extract_task = PythonOperator(
        task_id="doaj_capture",
        python_callable=run_doaj_capture,
    )
