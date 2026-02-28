"""Base extractor module for ETL processes."""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from bson import BSON
from pymongo import MongoClient
from pymongo.errors import PyMongoError

UTC = getattr(datetime, "UTC", timezone(timedelta(0)))


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.

    Provides common functionality for MongoDB connections, checkpointing,
    and logging. All concrete extractors should inherit from this class.

    Parameters
    ----------
    mongodb_uri : str
        MongoDB connection URI
    db_name : str
        Database name
    collection_name : str
        Collection name for storing extracted data
    checkpoint_collection : str, optional
        Collection name for storing ETL checkpoints (default: "etl_checkpoints")
    client : pymongo.MongoClient, optional
        Existing MongoDB client instance
    """

    def __init__(
        self,
        mongodb_uri: str,
        db_name: str,
        collection_name: str,
        checkpoint_collection: str = "etl_checkpoints",
        client: Any = None,
    ):
        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.checkpoint_collection_name = checkpoint_collection

        if client:
            self.client = client
        else:
            self.client = MongoClient(self.mongodb_uri)

        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.checkpoints = self.db[self.checkpoint_collection_name]

        self.logger = logging.getLogger(f"airflow.task.{self.__class__.__name__}")

    def get_checkpoint(self, source_id: str) -> Any | None:
        """
        Retrieve the last checkpoint for a given source.

        Parameters
        ----------
        source_id : str
            Unique identifier for the data source

        Returns
        -------
        Any or None
            Last checkpoint value if exists, None otherwise
        """
        checkpoint = self.checkpoints.find_one({"_id": source_id})
        return checkpoint.get("last_value") if checkpoint else None

    def save_checkpoint(self, source_id: str, value: Any) -> None:
        """
        Save checkpoint for a given source.

        Parameters
        ----------
        source_id : str
            Unique identifier for the data source
        value : Any
            Checkpoint value to store
        """
        self.checkpoints.update_one(
            {"_id": source_id}, {"$set": {"last_value": value}}, upsert=True
        )

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Main execution method to be implemented by subclasses.

        Parameters
        ----------
        *args : Any
            Positional arguments
        **kwargs : Any
            Keyword arguments

        Returns
        -------
        Any
            Extraction result
        """
        pass

    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()

    def dump_collection_if_exists(
        self, dump_dir: str, filename_prefix: str | None = None
    ) -> str | None:
        """
        Dump the current collection to a BSON file if it has documents.

        Parameters
        ----------
        dump_dir : str
            Directory where the dump file will be written.
        filename_prefix : str, optional
            File prefix for the dump file name.
        """
        if not dump_dir:
            return None

        try:
            count = self.collection.estimated_document_count()
        except PyMongoError:
            count = self.collection.count_documents({})

        if count == 0:
            return None

        Path(dump_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        prefix = filename_prefix or self.collection_name
        file_path = os.path.join(dump_dir, f"{prefix}_{timestamp}.bson")

        self.logger.info(f"Dumping {count} docs from {self.collection_name} to {file_path}...")
        with open(file_path, "wb") as handle:
            cursor = self.collection.find({})
            for doc in cursor:
                handle.write(BSON.encode(doc))

        return file_path
