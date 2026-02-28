from datetime import datetime
from typing import Any, cast
from unittest.mock import patch

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from dags.ciarp_capture import CiarpExtractor

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
    with patch.object(CiarpExtractor, "create_indexes"):
        return CiarpExtractor(
            mongodb_uri="mongodb://localhost:27017",
            db_name="institutional_test_db",
            drive_root_folder_id="root_folder_id",
            google_token_pickle=str(tmp_path / "drive_token.pickle"),
            collection_name="ciarp",
            client=mock_mongo,
            cache_dir=str(tmp_path / "ciarp_cache"),
            keep_only_latest_per_institution=True,
            backup_existing=False,
        )


def test_extractor_initialization(extractor):
    assert extractor.db_name == "institutional_test_db"
    assert extractor.collection_name == "ciarp"
    assert extractor.collection is not None
    assert extractor.checkpoints is not None


def test_parse_ciarp_filename(extractor):
    ror_id, file_date, date_raw = extractor._parse_ciarp_filename(
        "ciarp_006jxzx88_27_01_2026_14:41.xlsx"
    )
    assert ror_id == "006jxzx88"
    assert date_raw == "27_01_2026_14:41"
    assert file_date == datetime(2026, 1, 27, 14, 41)

    ror_id3, file_date3, date_raw3 = extractor._parse_ciarp_filename(
        "ciarp_006jxzx88_2026-01-28_10:06.xlsx"
    )
    assert ror_id3 == "006jxzx88"
    assert date_raw3 == "2026-01-28_10:06"
    assert file_date3 == datetime(2026, 1, 28, 10, 6)

    ror_id2, file_date2, date_raw2 = extractor._parse_ciarp_filename("bad_name.xlsx")
    assert ror_id2 is None
    assert file_date2 is None
    assert date_raw2 is None


def test_pick_latest_by_modified_time(extractor):
    files = [
        {
            "id": "a",
            "name": "ciarp_006jxzx88_01_01_2026_10:00.xlsx",
            "modifiedTime": "2026-01-01T15:00:00Z",
        },
        {
            "id": "b",
            "name": "ciarp_006jxzx88_27_01_2026_14:41.xlsx",
            "modifiedTime": "2026-01-27T19:41:00Z",
        },
        {
            "id": "c",
            "name": "ciarp_006jxzx88_15_01_2026_12:00.xlsx",
            "modifiedTime": "2026-01-15T17:00:00Z",
        },
    ]
    latest = extractor._pick_latest_by_modified_time(files)
    assert latest["id"] == "b"


def test_replace_institution_data_deletes_and_inserts(extractor):
    extractor.collection.insert_many(
        [
            {"_id": "006jxzx88:oldfile:0", "institution_id": "006jxzx88", "x": 1},
            {"_id": "006jxzx88:oldfile:1", "institution_id": "006jxzx88", "x": 2},
        ]
    )
    assert extractor.collection.count_documents({"institution_id": "006jxzx88"}) == 2

    file_meta = {
        "id": "newfileid",
        "name": "ciarp_006jxzx88_27_01_2026_14:41.xlsx",
        "modifiedTime": "2026-01-27T19:41:00Z",
        "size": "12345",
    }
    records = [
        {"col_a": "a1", "col_b": "b1"},
        {"col_a": "a2", "col_b": "b2"},
        {"col_a": "a3", "col_b": None},
    ]

    extractor._replace_institution_data(
        institution_id="006jxzx88",
        institution_name="Bond University",
        folder_id="folder_inst_1",
        file_meta=file_meta,
        records=records,
        file_date=datetime(2026, 1, 27, 14, 41),
        file_date_raw="27_01_2026_14:41",
        chunk_size=2,
    )

    assert extractor.collection.count_documents({"institution_id": "006jxzx88"}) == 3

    doc0 = extractor.collection.find_one({"_id": "006jxzx88:newfileid:0"})
    assert doc0 is not None
    assert doc0["institution_id"] == "006jxzx88"
    assert doc0["institution_name"] == "Bond University"
    assert doc0["drive_file_id"] == "newfileid"
    assert doc0["drive_modified_time"] == "2026-01-27T19:41:00Z"
    assert doc0["ciarp_file_date"] == "2026-01-27T14:41:00"
    assert doc0["row_number"] == 0
    assert doc0["col_a"] == "a1"


def test_process_all_files_smoke(extractor):
    folders = [
        {"id": "folder1", "name": "006jxzx88_Bond-University"},
        {"id": "folder2", "name": "02abcde12_Otra_Universidad"},
    ]

    excels_folder1 = [
        {
            "id": "file_old",
            "name": "ciarp_006jxzx88_01_01_2026_10:00.xlsx",
            "modifiedTime": "2026-01-01T15:00:00Z",
        },
        {
            "id": "file_latest",
            "name": "ciarp_006jxzx88_2026-01-28_10:06.xlsx",
            "modifiedTime": "2026-01-28T15:06:00Z",
        },
    ]
    excels_folder2 = [
        {
            "id": "file2_latest",
            "name": "ciarp_02abcde12_2026-01-20_09:00.xlsx",
            "modifiedTime": "2026-01-20T14:00:00Z",
        }
    ]

    def fake_already_loaded(institution_id, drive_file_id, drive_modified_time):
        return institution_id == "006jxzx88" and drive_file_id == "file_latest"

    with (
        patch.object(extractor, "_list_institution_folders", return_value=folders),
        patch.object(
            extractor,
            "_list_ciarp_excels",
            side_effect=lambda fid: excels_folder1 if fid == "folder1" else excels_folder2,
        ),
        patch.object(extractor, "_download_file_to_cache", return_value="/tmp/fake.xlsx"),
        patch.object(extractor, "_read_excel_as_records", return_value=[{"a": "1"}, {"a": "2"}]),
        patch.object(extractor, "_already_loaded", side_effect=fake_already_loaded),
        patch.object(extractor, "_backup_collection_if_needed"),
    ):
        stats = extractor.process_all_files(force=False)

    assert stats["folders"] == 2
    assert stats["processed"] == 1
    assert stats["skipped"] == 1
    assert stats["errors"] == 0

    assert extractor.collection.count_documents({"institution_id": "02abcde12"}) == 2
    assert extractor.collection.count_documents({"institution_id": "006jxzx88"}) == 0
