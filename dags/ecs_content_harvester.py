from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_content_harvester import ContentHarvestEcsOperator


@dag(
    dag_id="ecs_content_harvest",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': 
        Param(None, description="Collection ID to harvest_content"),
        'page_filename':
        Param(None, description="Page filename to harvest_content")
    },
    tags=["dev"],
)
def ecs_content_harvest():
    harvest_content_for_page = ContentHarvestEcsOperator(
        task_id="page_content_harvester_on_ecs",
        collection_id="{{ params.collection_id }}",
        page="{{ params.page_filename }}",
    )
    harvest_content_for_page

    harvest_content_for_collection = ContentHarvestEcsOperator(
        task_id = "collection_content_harvester_on_ecs",
        overrides = {
            "containerOverrides": [
                {
                    "name": "rikolti-content_harvester",
                    "command": [
                        "python3", "-m", "content_harvester.by_collection", 
                        "{{params.collection_id}}"
                    ],
                }
            ]
        },
        collection_id = "{{ params.collection_id }}",
        page="all"
    )
    harvest_content_for_collection

ecs_content_harvest()
