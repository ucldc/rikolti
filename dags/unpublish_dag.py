from datetime import datetime

from airflow.sdk import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.indexing_tasks import unpublish_collection_task
from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.dags.shared_tasks.shared import notify_dag_success
from rikolti.dags.shared_tasks.shared import notify_dag_failure


@dag(
    dag_id="unpublish_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to unpublish"),
    },
    tags=["rikolti"],
    on_failure_callback=notify_dag_failure,
    on_success_callback=notify_dag_success,
)
def unpublish_collection_dag():
    collection = get_registry_data_task()
    unpublish_collection_task(collection)

unpublish_collection_dag()