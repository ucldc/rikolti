import logging

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator

from rikolti.dags.utils_by_mapper_type import make_mapper_type_endpoint
from rikolti.dags.utils_by_mapper_type import fetch_endpoint_task
from rikolti.dags.utils_by_mapper_type import map_endpoint_task
from rikolti.dags.utils_by_mapper_type import validate_endpoint_task
from rikolti.dags.shared_tasks import s3_to_localfilesystem

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="dev_validate_by_mapper_type",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'mapper_type': Param(None, description="Rikolti mapper type to harvest and validate"),
        'limit': Param(None, description="Limit number of collections to validate"),
    },
    tags=["dev"],
)
def dev_validate_by_mapper_type():
    endpoint=make_mapper_type_endpoint()
    validation_reports = validate_endpoint_task(endpoint)
    (
        fetch_endpoint_task(endpoint) >>
        map_endpoint_task(endpoint) >>
        validation_reports
    )

    local_filepaths = s3_to_localfilesystem.expand(
        s3_url=validation_reports)
    upload_validation_files = LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id="google_cloud_default",
        task_id="upload_validation_files",
        local_paths=validation_reports,
        drive_folder="rikolti_validation_folder",
        ignore_if_missing=True,
    )
    local_filepaths >> upload_validation_files


dev_validate_by_mapper_type()