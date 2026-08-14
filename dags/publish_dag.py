# ruff: noqa: DTZ001

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.indexing_tasks import (
    get_version_pages,
    publish_collection_task,
)
from rikolti.dags.shared_tasks.shared import (
    get_registry_data_task,
    notify_dag_failure,
    notify_dag_success,
)


@dag(
    dag_id="publish_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to publish"),
        'version': Param(None, description="Version path to publish")
    },
    tags=["rikolti"],
    on_failure_callback=notify_dag_failure,
    on_success_callback=notify_dag_success,
)
def publish_collection_dag():
    collection = get_registry_data_task()
    version_pages = get_version_pages()
    publish_collection_task(collection, version_pages)

publish_collection_dag()