import os
import json

from airflow.decorators import task

from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import send_event_to_sns
from rikolti.record_indexer.create_collection_index import create_index_name
from rikolti.record_indexer.create_collection_index import create_new_index
from rikolti.record_indexer.create_collection_index import delete_index
from rikolti.record_indexer.move_index_to_prod import move_index_to_prod
from rikolti.record_indexer.update_stage_index import update_stage_index_for_collection
from rikolti.utils.versions import get_version

@task(task_id="create_stage_index", on_failure_callback=notify_rikolti_failure)
def create_stage_index_task(
    collection: dict, version_pages: list[str], **context):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    index_name = create_index_name(collection_id)
    try:
        create_new_index(collection_id, version_pages, index_name)
    except Exception as e:
        delete_index(index_name)
        raise e

    version = get_version(collection_id, version_pages[0])
    dashboard_query = {"query": {
        "bool": {"filter": {"terms": {"collection_url": [collection_id]}}}
    }}
    print(
        f"\n\nReview indexed records at: https://rikolti-data.s3.us-west-2."
        f"amazonaws.com/index.html#{version.rstrip('/')}/data/ \n\n"
        f"Or on opensearch at: {os.environ.get('RIKOLTI_ES_ENDPOINT')}"
        "/_dashboards/app/dev_tools#/console with query:\n"
        f"{json.dumps(dashboard_query, indent=2)}\n\n\n"
    )

    send_event_to_sns(context, {'index_name': index_name})
    return index_name

# this task has the same task_id as create_stage_index_task() so that we don't
# break the Airflow history with the transition between using that task
# and this one as the last step in the harvest_dag
@task(task_id="create_stage_index", on_failure_callback=notify_rikolti_failure)
def update_stage_index_for_collection_task(
    collection: dict, version_pages: list[str], **context):

    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    try:
        update_stage_index_for_collection(collection_id, version_pages)
    except Exception as e:
        # TODO: implement some rollback exception handling?
        raise e

    version = get_version(collection_id, version_pages[0])
    dashboard_query = {"query": {
        "bool": {"filter": {"terms": {"collection_url": [collection_id]}}}
    }}
    print(
        f"\n\nReview indexed records at: https://rikolti-data.s3.us-west-2."
        f"amazonaws.com/index.html#{version.rstrip('/')}/data/ \n\n"
        f"Or on opensearch at: {os.environ.get('RIKOLTI_ES_ENDPOINT')}"
        "/_dashboards/app/dev_tools#/console with query:\n"
        f"{json.dumps(dashboard_query, indent=2)}\n\n\n"
    )

    send_event_to_sns(context, {'record_indexer_success': 'success'})

@task(task_id="move_index_to_prod")
def move_index_to_prod_task(collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    move_index_to_prod(collection_id)
