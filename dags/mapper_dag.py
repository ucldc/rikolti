from datetime import datetime
import json
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.metadata_mapper.lambda_shepherd import get_vernacular_pages, get_collection
from rikolti.metadata_mapper.lambda_function import map_page as lambda_map_page


@task()
def fetch_pages(params=None):
    if not params:
        return False

    payload = params.get('payload')
    collection_id = payload.get('collection_id', 0)
    # raise an error?
    if not collection_id:
        return []

    pages = get_vernacular_pages(
                collection_id)

    return pages

@task()
def map_page(page: str, params=None):
    if not params:
        return False

    payload = params.get('payload')
    collection_id = payload.get('collection_id', 0)
    # raise an error?
    if not collection_id:
        return {}

    collection = get_collection(collection_id)
    payload.update({'collection': collection})
    payload.update({'page_filename': page})

    try:
        mapped_page = lambda_map_page(json.dumps(payload), {})
    except KeyError:
        print(
            f"[{collection_id}]: {collection['rikolti_mapper_type']} "
            "not yet implemented", file=sys.stderr
        )

    return mapped_page

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'payload': Param(
            {
                "collection_id": 26284, 
                "rikolti_mapper_type": "oai.chapman", 
                "page_filename": "0"
            }, 
            description="Payload from Collection Registry API"
        )},
    tags=["rikolti"],
)
def mapper_dag():
    # simple dynamic task mapping
    # max_map_length=1024 by default. 
    # if fetch_pages() generates more than this, that task will fail
    # need to somehow chunk up pages into groups of 1024?
    map_page.expand(page=fetch_pages())
    
    # max_active_tis_per_dag - setting on the task to restrict how many
    # instances can be running at the same time, *across all DAG runs*

mapper_dag()
