import logging

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.shared import make_registry_endpoint_task
from rikolti.dags.shared_tasks.fetching_tasks import fetch_endpoint_task
from rikolti.dags.shared_tasks.mapping_tasks import map_endpoint_task
from rikolti.dags.shared_tasks.mapping_tasks import validate_endpoint_task

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="validate_by_mapper_type",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'mapper_type': Param(
            None, description=("Legacy mapper type to harvest and validate; "
                               "use mapper_type, rikolti_mapper_type, OR "
                               "endpoint")),
        'rikolti_mapper_type': Param(
            None, description=("Rikolti mapper type to harvest and validate; "
                               "use mapper_type, rikolti_mapper_type, OR "
                               "endpoint")),
        'registry_api_queryset': Param(
            None, description=("Registry endpoint to harvest and validate; "
                               "use mapper_type, rikolti_mapper_type, OR "
                               "endpoint")),
        'limit': Param(
            None, description="Limit number of collections to validate"),
        'offset': Param(None, description="Position to start at")
    },
    tags=["rikolti"],
)
def validate_by_mapper_type():
    endpoint=make_registry_endpoint_task()
    fetched_versions = fetch_endpoint_task(endpoint)
    mapped_versions = map_endpoint_task(endpoint, fetched_versions)
    validation_reports = validate_endpoint_task(endpoint, mapped_versions)  # noqa: F841

validate_by_mapper_type()