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
        Param(None, description="Page filename to harvest_content"),
        'mapper_type':
        Param(None, description="Ignored unless 'nuxeo.nuxeo'"),
        'with_content_urls_version':
        Param(None, description="with_content_urls version path")
    },
    tags=["dev"],
)
def ecs_content_harvest():
    harvest_content_for_page = ContentHarvestEcsOperator(
        task_id="page_content_harvester_on_ecs",
        collection_id="{{ params.collection_id }}",
        with_content_urls_version="{{ params.with_content_urls_version }}",
        pages="{{ params.page_filename }}",
        mapper_type="{{ params.mapper_type }}",
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
        with_content_urls_version="{{ params.with_content_urls_version }}",
        pages="all",
        mapper_type="{{ params.mapper_type }}",
    )
    harvest_content_for_collection

ecs_content_harvest()
