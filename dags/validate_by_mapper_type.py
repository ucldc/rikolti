import logging

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.utils_by_mapper_type import make_mapper_type_endpoint
from rikolti.dags.utils_by_mapper_type import fetch_endpoint_task
from rikolti.dags.utils_by_mapper_type import map_endpoint_task
from rikolti.dags.utils_by_mapper_type import validate_endpoint_task

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="validate_by_mapper_type",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'mapper_type': Param(None,
                             description="Legacy mapper type to harvest and validate"),
        'limit': Param(None, description="Limit number of collections to validate"),
    },
    tags=["rikolti"],
)
def validate_by_mapper_type():
    endpoint=make_mapper_type_endpoint()
    validation_reports = validate_endpoint_task(endpoint)
    (
        fetch_endpoint_task(endpoint) >>
        map_endpoint_task(endpoint) >>
        validation_reports
    )

validate_by_mapper_type()