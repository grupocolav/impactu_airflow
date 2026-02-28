from typing import Any, cast
from unittest.mock import patch

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from dags.staff_capture import StaffExtractor

# Workaround for mongomock bug with pymongo 4.x bulk_write (seen in some bulk ops)
# https://github.com/mongomock/mongomock/issues/812
_builder_cls = cast(Any, BulkOperationBuilder)
if hasattr(_builder_cls, "add_update"):
    original_add_update = _builder_cls.add_update

    def patched_add_update(self, filter, doc, upsert=False, multi=False, **kwargs):
        kwargs.pop("sort", None)
        return original_add_update(self, filter, doc, upsert, multi, **kwargs)

    _builder_cls.add_update = patched_add_update

if hasattr(_builder_cls, "add_replace"):
    original_add_replace = _builder_cls.add_replace

    def patched_add_replace(self, selector, doc, upsert=False, **kwargs):
        kwargs.pop("sort", None)
        return original_add_replace(self, selector, doc, upsert=upsert, **kwargs)

    _builder_cls.add_replace = patched_add_replace


@pytest.fixture
def mock_mongo():
    return mongomock.MongoClient()


@pytest.fixture
def extractor(mock_mongo, tmp_path):
    # Patch create_indexes to avoid any mongomock index edge-cases during init
    with patch.object(StaffExtractor, "create_indexes"):
        return StaffExtractor(
            mongodb_uri="mongodb://localhost:27017",
            db_name="institutional_test_db",
            drive_root_folder_id="root_folder_id",
            google_token_pickle=str(tmp_path / "drive_token.pickle"),
            collection_name="staff",
            client=mock_mongo,
            cache_dir=str(tmp_path / "staff_cache"),
            keep_only_latest_per_institution=True,
        )


def test_extractor_initialization(extractor):
    assert extractor.db_name == "institutional_test_db"
    assert extractor.collection_name == "staff"
    assert extractor.collection is not None
    assert extractor.checkpoints is not None


def test_parse_institution_folder_name(extractor):
    ror_id, inst_name = extractor._parse_institution_folder_name(
        "03bp5hc83_Universidad_de_Antioquia"
    )
    assert ror_id == "03bp5hc83"
    assert inst_name == "Universidad_de_Antioquia"

    ror_id2, inst_name2 = extractor._parse_institution_folder_name("BadFormatName")
    assert ror_id2 is None
    assert inst_name2 == "BadFormatName"


def test_pick_latest_by_modified_time(extractor):
    files = [
        {"id": "a", "name": "staff_a.xlsx", "modifiedTime": "2025-01-01T00:00:00Z"},
        {"id": "b", "name": "staff_b.xlsx", "modifiedTime": "2025-02-01T00:00:00Z"},
        {"id": "c", "name": "staff_c.xlsx", "modifiedTime": "2024-12-31T23:59:59Z"},
    ]
    latest = extractor._pick_latest_by_modified_time(files)
    assert latest["id"] == "b"


def test_replace_institution_data_deletes_and_inserts(extractor):
    # Pre-insert old docs for the same institution to verify deletion behavior
    extractor.collection.insert_many(
        [
            {"_id": "03bp5hc83:oldfile:0", "institution_id": "03bp5hc83", "x": 1},
            {"_id": "03bp5hc83:oldfile:1", "institution_id": "03bp5hc83", "x": 2},
        ]
    )
    assert extractor.collection.count_documents({"institution_id": "03bp5hc83"}) == 2

    file_meta = {
        "id": "newfileid",
        "name": "staff_03bp5hc83_02_10_2025_14_54.xlsx",
        "modifiedTime": "2025-10-02T19:54:00Z",
        "size": "12345",
    }
    records = [
        {"col_a": "a1", "col_b": "b1"},
        {"col_a": "a2", "col_b": "b2"},
        {"col_a": "a3", "col_b": None},
    ]

    extractor._replace_institution_data(
        institution_id="03bp5hc83",
        institution_name="Universidad de Antioquia",
        folder_id="folder_inst_1",
        file_meta=file_meta,
        records=records,
        chunk_size=2,
    )

    # Should now have only the new 3 docs for this institution (old ones deleted)
    assert extractor.collection.count_documents({"institution_id": "03bp5hc83"}) == 3

    doc0 = extractor.collection.find_one({"_id": "03bp5hc83:newfileid:0"})
    assert doc0 is not None
    assert doc0["institution_id"] == "03bp5hc83"
    assert doc0["institution_name"] == "Universidad de Antioquia"
    assert doc0["drive_file_id"] == "newfileid"
    assert doc0["drive_modified_time"] == "2025-10-02T19:54:00Z"
    assert doc0["row_number"] == 0
    assert doc0["col_a"] == "a1"


def test_already_loaded(extractor):
    extractor.collection.insert_one(
        {
            "_id": "03bp5hc83:fileX:0",
            "institution_id": "03bp5hc83",
            "drive_file_id": "fileX",
            "drive_modified_time": "2025-01-01T00:00:00Z",
            "row_number": 0,
        }
    )

    assert extractor._already_loaded("03bp5hc83", "fileX", "2025-01-01T00:00:00Z") is True
    assert extractor._already_loaded("03bp5hc83", "fileX", "2025-01-02T00:00:00Z") is False


def test_process_all_institutions_smoke(extractor):
    """
    End-to-end (mocked) test:
    - 2 institution folders
    - 1 is skipped because latest file already loaded
    - 1 is processed and writes records
    """
    folders = [
        {"id": "folder1", "name": "03bp5hc83_Universidad_de_Antioquia"},
        {"id": "folder2", "name": "02abcde12_Otra_Universidad"},
    ]

    # Each folder has "excel files"; pick_latest will choose by modifiedTime
    excels_folder1 = [
        {
            "id": "file_old",
            "name": "staff_03bp5hc83_old.xlsx",
            "modifiedTime": "2025-01-01T00:00:00Z",
        },
        {
            "id": "file_latest",
            "name": "staff_03bp5hc83_latest.xlsx",
            "modifiedTime": "2025-02-01T00:00:00Z",
        },
    ]
    excels_folder2 = [
        {
            "id": "file2_latest",
            "name": "staff_02abcde12_latest.xlsx",
            "modifiedTime": "2025-03-01T00:00:00Z",
        }
    ]

    # Simulate: folder1 is already loaded, folder2 needs processing
    def fake_already_loaded(institution_id, drive_file_id, drive_modified_time):
        return institution_id == "03bp5hc83" and drive_file_id == "file_latest"

    with (
        patch.object(extractor, "_list_institution_folders", return_value=folders),
        patch.object(
            extractor,
            "_list_staff_excels",
            side_effect=lambda fid: excels_folder1 if fid == "folder1" else excels_folder2,
        ),
        patch.object(extractor, "_download_file_to_cache", return_value="/tmp/fake.xlsx"),
        patch.object(
            extractor,
            "_read_excel_as_records",
            return_value=[{"a": "1"}, {"a": "2"}],
        ),
        patch.object(extractor, "_already_loaded", side_effect=fake_already_loaded),
    ):
        stats = extractor.process_all_institutions(force=False)

    assert stats["folders"] == 2
    assert stats["processed"] == 1
    assert stats["skipped"] == 1
    assert stats["errors"] == 0

    # Only folder2 should have been written
    assert extractor.collection.count_documents({"institution_id": "02abcde12"}) == 2
    assert extractor.collection.count_documents({"institution_id": "03bp5hc83"}) == 0
