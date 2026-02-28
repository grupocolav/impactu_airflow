from typing import Any, cast
from unittest.mock import patch

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from dags.doaj_capture import DoajExtractor

# Workaround for mongomock bug with pymongo 4.x bulk_write
_builder_cls = cast(Any, BulkOperationBuilder)
if hasattr(_builder_cls, "add_update"):
    original_add_update = _builder_cls.add_update

    def patched_add_update(self, filter, doc, upsert=False, multi=False, **kwargs):
        kwargs.pop("sort", None)
        return original_add_update(self, filter, doc, upsert, multi, **kwargs)

    _builder_cls.add_update = patched_add_update


@pytest.fixture
def mock_mongo():
    return mongomock.MongoClient()


@pytest.fixture
def extractor(mock_mongo, tmp_path):
    with patch.object(DoajExtractor, "create_indexes"):
        return DoajExtractor(
            mongodb_uri="mongodb://localhost:27017",
            db_name="doaj_test_db",
            client=mock_mongo,
            cache_dir=str(tmp_path),
        )


def test_derive_fields_article_with_doi():
    record = {
        "id": "abc123",
        "bibjson": {
            "identifier": [{"type": "doi", "id": "10.1234/ABC"}],
            "publisher": "Test Publisher",
            "year": 2024,
        },
    }

    derived = DoajExtractor._derive_fields(record, "article")
    assert derived["source_id"] == "doi:10.1234/abc"
    assert derived["doi"] == "10.1234/ABC"
    assert derived["doaj_id"] == "abc123"
    assert derived["publisher"] == "Test Publisher"
    assert derived["year"] == 2024


def test_derive_fields_journal_with_doaj_id():
    record = {"id": "journal-001", "bibjson": {"pissn": "1234-5678"}}
    derived = DoajExtractor._derive_fields(record, "journal")
    assert derived["source_id"] == "doaj:journal-001"
    assert derived["pissn"] == "1234-5678"


def test_iter_records_ndjson(tmp_path):
    content = '{"id": "1"}\n{"id": "2"}\n'
    file_path = tmp_path / "article_batch_1.json"
    file_path.write_text(content, encoding="utf-8")

    records = list(DoajExtractor._iter_records(str(file_path)))
    assert len(records) == 2
    assert records[0]["id"] == "1"
    assert records[1]["id"] == "2"


def test_process_file_upserts(extractor, tmp_path):
    content = '{"id": "1", "bibjson": {"identifier": [{"type": "doi", "id": "10.1/a"}]}}\n'
    content += '{"id": "2", "bibjson": {"identifier": [{"type": "doi", "id": "10.1/b"}]}}\n'
    file_path = tmp_path / "article_batch_1.json"
    file_path.write_text(content, encoding="utf-8")

    counts = extractor._process_file(str(file_path), dump_type="article", chunk_size=1)
    assert counts["records"] == 2
    assert counts["upserted"] == 2

    collection = extractor.db["articles"]
    assert collection.count_documents({}) == 2
    assert collection.find_one({"_id": "doi:10.1/a"}) is not None
