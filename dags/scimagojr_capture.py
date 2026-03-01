"""ScimagoJR data extraction DAG (renamed to scimagojr_capture)."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

# Note: import `ScimagoJRExtractor` inside the task to avoid import-time
# side-effects when Airflow parses DAGs. This prevents module-level
# filesystem changes from triggering linter errors during import.

default_args = {
    "owner": "impactu",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(hours=1),
}


def run_extraction_by_year(
    year: int,
    mongo_conn_id: str = "mongodb_default",
    mongo_db: str | None = None,
    **kwargs: dict,
) -> None:
    """
    Extract ScimagoJR data for a specific year.
    """
    # Get params from DAG
    force_redownload = kwargs["params"].get("force_redownload", True)
    chunk_size = kwargs["params"].get("chunk_size", 1000)

    # Resolve Mongo connection from Airflow hook if provided. Require `mongo_db`.
    hook = MongoHook(mongo_conn_id=mongo_conn_id)
    client = hook.get_conn()
    if mongo_db is None:
        conn = getattr(hook, "connection", None)
        mongo_db = getattr(conn, "schema", None) if conn is not None else None
        if not mongo_db:
            raise ValueError(
                "MongoDB database not provided. Set `mongo_db` in DAG params or provide it in the connection schema."
            )

    # Import extractor here to avoid module-level import side-effects
    from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor

    # Use AIRFLOW_HOME for cache directory to avoid writing to /opt/airflow
    airflow_home = os.environ.get("AIRFLOW_HOME", os.getcwd())
    cache_dir = os.path.join(airflow_home, "cache", "scimagojr")
    extractor = ScimagoJRExtractor("", mongo_db, client=client, cache_dir=cache_dir)
    try:
        extractor.process_year(year, force_redownload=force_redownload, chunk_size=chunk_size)
    finally:
        extractor.close()


with DAG(
    "scimagojr_capture",
    default_args=default_args,
    description="Extract data from ScimagoJR and load into MongoDB",
    schedule="@monthly",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["extract", "scimagojr"],
    params={
        "mongo_conn_id": Param("mongodb_default", type="string", description="Mongo connection id"),
        "mongo_db": Param(
            "scimagojr", type="string", description="MongoDB database name (required)"
        ),
        "force_redownload": Param(
            True,
            type="boolean",
            description="Force data download even if already in cache or database",
        ),
        "chunk_size": Param(
            1000, type="integer", description="Number of records to insert in each bulk operation"
        ),
    },
) as dag:
    years = list(range(1999, datetime.now().year + 1))

    extract_task = PythonOperator.partial(
        task_id="extract_and_load_scimagojr",
        python_callable=run_extraction_by_year,
    ).expand(
        op_kwargs=[
            {
                "year": year,
                "mongo_conn_id": "{{ params.mongo_conn_id }}",
                "mongo_db": "{{ params.mongo_db }}",
            }
            for year in years
        ]
    )
