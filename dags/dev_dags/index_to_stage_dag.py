from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.dags.shared_tasks.indexing_tasks import create_stage_index_task
from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.utils.versions import get_merged_pages, get_with_content_urls_pages


@task(task_id="get_version_pages")
def get_version_pages(params=None):
    if not params or not params.get('version'):
        raise ValueError("Version path not found in params")
    version = params.get('version')

    if 'merged' in version:
        version_pages = get_merged_pages(version)
    else:
        version_pages = get_with_content_urls_pages(version)

    return version_pages


@dag(
    dag_id="index_collection_to_stage",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to index"),
        'version': Param(None, description="Version path to index")
    },
    tags=["rikolti"],
)
def index_collection_to_stage_dag():
    collection = get_registry_data_task()
    version_pages = get_version_pages()
    index_name = create_stage_index_task(collection, version_pages)  # noqa F841

index_collection_to_stage_dag()