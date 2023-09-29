from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from rikolti.dags.harvest_dag import get_collection_metadata_task
from rikolti.dags.harvest_dag import map_page_task
from rikolti.dags.harvest_dag import get_mapping_status_task
from rikolti.metadata_mapper.lambda_shepherd import get_vernacular_pages


@task()
def get_vernacular_pages_task(collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    pages = get_vernacular_pages(collection_id)
    return pages

# This is a functional duplicate of 
# rikolti.metadata_mapper.lambda_shepherd.map_collection

# Within an airflow runtime context, we take advantage of airflow's dynamic
# task mapping to fan out all calls to map_page. 
# Outside the airflow runtime context, on the command line for example, 
# map_collection performs manual "fan out" in the for loop below. 

# TODO: Any changes to mapper_dag should be carefully considered, duplicated
# to map_collection, and tested in both contexts. Resolve multiple contexts.

# TODO: this is a simple dynamic task mapping w/ max_map_length=1024 by default
# if get_vernacular_pages_task() returns more than 1024 pages, map_page_task
# will fail - need to somehow chunk up pages into groups of 1024?

@dag(
    dag_id="map_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to map")},
    tags=["rikolti"],
)
def mapper_dag():
    collection = get_collection_metadata_task()
    page_list = get_vernacular_pages_task(collection=collection)
    mapped_pages = (
        map_page_task
            .partial(collection=collection)
            .expand(page=page_list)
    )

    get_mapping_status_task(collection, mapped_pages)
mapper_dag()
