import os

from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks import ContentHarvestDockerOperator

@dag(
    dag_id="content_harvest",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(27425, description="Collection ID to harvet_content")},
    tags=["rikolti"],
)
def content_harvest():
    content_harvester_task = ContentHarvestDockerOperator(
        task_id="page_content_harvester",
        collection_id="{{ params.collection_id }}",
        page=0,
    )
    content_harvester_task

    collection_content_harvester_task = ContentHarvestDockerOperator(
        task_id="collection_content_harvester",
        entrypoint="python3 -m content_harvester.by_collection",
        command=["{{ params.collection_id }}"],
        collection_id="{{ params.collection_id }}",
        page="all",
    )
    collection_content_harvester_task

content_harvest()
