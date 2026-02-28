from typing import Any, cast
from unittest.mock import MagicMock, patch

import mongomock
import pytest
from mongomock.collection import BulkOperationBuilder

from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor

# Workaround for mongomock bug with pymongo 4.x bulk_write
# https://github.com/mongomock/mongomock/issues/812
_builder_cls = cast(Any, BulkOperationBuilder)
if hasattr(_builder_cls, "add_update"):
    original_add_update = _builder_cls.add_update

    def patched_add_update(self, filter, doc, upsert=False, multi=False, **kwargs):
        kwargs.pop("sort", None)
        return original_add_update(self, filter, doc, upsert, multi, **kwargs)

    _builder_cls.add_update = patched_add_update


@pytest.fixture
def mock_mongo():
    client = mongomock.MongoClient()
    return client


@pytest.fixture
def extractor(mock_mongo):
    return ScimagoJRExtractor(
        mongodb_uri="mongodb://localhost:27017",
        db_name="test_db",
        client=mock_mongo,
        cache_dir="/tmp/scimagojr_test_cache",
    )


def test_extractor_initialization(extractor):
    assert extractor.db_name == "test_db"
    assert extractor.collection_name == "scimagojr"
    # Check if indexes were "created" (mongomock doesn't strictly enforce them but we call the method)
    assert extractor.collection is not None


@patch("requests.get")
def test_fetch_year(mock_get, extractor):
    # Mock CSV response
    csv_content = "Rank;Sourceid;Title;Type;Issn;SJR;SJR Best Quartile;H index;Total Docs. (2023);Total Docs. (3years);Total Refs.;Total Cites (3years);Citable Docs. (3years);Cites / Doc. (2years);Ref. / Doc.;Country;Region;Publisher;Coverage;Categories\n1;12345;Journal of Tests;journal;12345678;1.5;Q1;50;100;300;5000;1000;280;3.5;50.0;Colombia;Latin America;Test Publisher;2020-2023;Test Category"  # noqa E501

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = csv_content
    mock_get.return_value = mock_response

    # Test fetching (force redownload to avoid cache issues in tests)
    data = extractor.fetch_year(2023, force_redownload=True)

    assert len(data) == 1
    assert data[0]["Sourceid"] == 12345
    assert data[0]["year"] == 2023
    assert data[0]["Rank"] == 1


def test_process_year_differential(extractor):
    # 1. Setup initial data in mock DB
    initial_record = {"Sourceid": 12345, "year": 2023, "Title": "Old Title", "Rank": 2}
    extractor.collection.insert_one(initial_record)

    # 2. Mock fetch_year to return updated data
    updated_data = [
        {"Sourceid": 12345, "year": 2023, "Title": "New Title", "Rank": 1},
        {"Sourceid": 67890, "year": 2023, "Title": "Brand New Journal", "Rank": 2},
    ]

    with patch.object(extractor, "fetch_year", return_value=updated_data):
        extractor.process_year(2023)

    # 3. Verify results
    # Should have 2 records now
    assert extractor.collection.count_documents({"year": 2023}) == 2

    # Check update
    updated = extractor.collection.find_one({"Sourceid": 12345})
    assert updated["Title"] == "New Title"
    assert updated["Rank"] == 1

    # Check insert
    new_rec = extractor.collection.find_one({"Sourceid": 67890})
    assert new_rec["Title"] == "Brand New Journal"


def test_cleanup_year(mock_mongo):
    # Create extractor without calling create_indexes to allow duplicates for this test
    with patch.object(ScimagoJRExtractor, "create_indexes"):
        extractor = ScimagoJRExtractor(
            mongodb_uri="mongodb://localhost:27017",
            db_name="test_db_cleanup",
            client=mock_mongo,
            cache_dir="/tmp/scimagojr_test_cache_cleanup",
        )

    # Insert duplicates manually
    records = [
        {"Sourceid": 1, "year": 2023, "data": "a"},
        {"Sourceid": 1, "year": 2023, "data": "b"},  # Duplicate
        {"Sourceid": 2, "year": 2023, "data": "c"},
    ]
    extractor.collection.insert_many(records)

    assert extractor.collection.count_documents({"year": 2023}) == 3

    # Run cleanup
    extractor.cleanup_year(2023, expected_count=2)

    assert extractor.collection.count_documents({"year": 2023}) == 2
    # Ensure we still have both unique Sourceids
    assert extractor.collection.count_documents({"Sourceid": 1}) == 1
    assert extractor.collection.count_documents({"Sourceid": 2}) == 1
