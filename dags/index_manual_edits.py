from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.utils.versions import get_versioned_pages
from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.dags.shared_tasks.shared import notify_dag_success
from rikolti.dags.shared_tasks.shared import notify_dag_failure
from rikolti.dags.shared_tasks.indexing_tasks import stage_collection_task

@task
def get_pages_task(params=None, **context):
    if not params:
        return []
    version = params.get('version')
    return get_versioned_pages(version)

@dag(
    dag_id="index_manual_edits",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to harvest"),
        'version': Param(None, description="Version path to index to stage, example: 28090/vernacular_metadata_2025-12-19T01:56:51/mapped_metadata_2025-12-19T01:57:02/with_content_urls_2025-12-19T21:20:15/"),
    },
    description="Index manual edits to rikolti data to the stage index",
    tags=["rikolti"],
    on_failure_callback=notify_dag_failure,
    on_success_callback=notify_dag_success,
)

def index_manual_edits():
    collection = get_registry_data_task()
    pages = get_pages_task()
    stage_collection_task(collection, pages)


index_manual_edits()