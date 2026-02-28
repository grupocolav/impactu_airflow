from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param

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
    "institutional_data_capture",
    default_args=default_args,
    description="Trigger institutional capture DAGs for STAFF and CIARP",
    render_template_as_native_obj=True,
    schedule=None,
    catchup=False,
    tags=["capture", "institutional"],
    params={
        "google_token_pickle": Param(
            "",
            type="string",
            description="Path to Google Drive credentials pickle file",
        ),
        "drive_root_folder_id": Param(
            "",
            type="string",
            description="Google Drive root folder ID containing staff/ciarp subfolders",
        ),
        "staff_drive_subfolder_name": Param(
            "staff",
            type="string",
            description="Optional subfolder name under staff root (e.g., Staff)",
        ),
        "ciarp_drive_subfolder_name": Param(
            "ciarp",
            type="string",
            description="Optional subfolder name under CIARP root (e.g., Ciarp)",
        ),
        "staff_cache_dir": Param(
            "/tmp/impactu_airflow_cache/staff",
            type="string",
            description="Local cache directory for STAFF downloads",
        ),
        "ciarp_cache_dir": Param(
            "/tmp/impactu_airflow_cache/ciarp",
            type="string",
            description="Local cache directory for CIARP downloads",
        ),
        "dump_dir": Param(
            "/tmp/impactu_airflow_cache/dumps",
            type="string",
            description="Optional dump directory for STAFF/CIARP collections",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Reprocess even if latest file was already loaded",
        ),
        "keep_only_latest_per_institution": Param(
            True,
            type="boolean",
            description="Delete previous docs for each institution before loading latest",
        ),
        "backup_existing": Param(
            True,
            type="boolean",
            description="Create a backup collection before loading if data exists (CIARP)",
        ),
    },
) as dag:
    trigger_staff_capture = TriggerDagRunOperator(
        task_id="trigger_staff_capture",
        trigger_dag_id="staff_capture",
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "drive_root_folder_id": "{{ params.drive_root_folder_id }}",
            "drive_subfolder_name": "{{ params.staff_drive_subfolder_name }}",
            "dump_dir": "{{ params.dump_dir }}",
            "google_token_pickle": "{{ params.google_token_pickle }}",
            "cache_dir": "{{ params.staff_cache_dir }}",
            "force": "{{ params.force }}",
            "keep_only_latest_per_institution": "{{ params.keep_only_latest_per_institution }}",
        },
    )

    trigger_ciarp_capture = TriggerDagRunOperator(
        task_id="trigger_ciarp_capture",
        trigger_dag_id="ciarp_capture",
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "drive_root_folder_id": "{{ params.drive_root_folder_id }}",
            "drive_subfolder_name": "{{ params.ciarp_drive_subfolder_name }}",
            "dump_dir": "{{ params.dump_dir }}",
            "google_token_pickle": "{{ params.google_token_pickle }}",
            "cache_dir": "{{ params.ciarp_cache_dir }}",
            "force": "{{ params.force }}",
            "keep_only_latest_per_institution": "{{ params.keep_only_latest_per_institution }}",
            "backup_existing": "{{ params.backup_existing }}",
        },
    )

    trigger_staff_capture >> trigger_ciarp_capture
