import os
import json

from airflow.decorators import task

from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import send_event_to_sns
from rikolti.record_indexer.index_collection import (
    index_collection, delete_collection)
from rikolti.utils.versions import (
    get_version, get_merged_pages, get_with_content_urls_pages)

def index_collection_task(alias, collection, version_pages, context):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    try:
        index_collection(alias, collection_id, version_pages)
    except Exception as e:
        # TODO: implement some rollback exception handling?
        raise e

    # Construct Airflow Log Message
    version = get_version(collection_id, version_pages[0])
    dashboard_query = {"query": {
        "bool": {"filter": {"terms": {"collection_url": [collection_id]}}}
    }}
    hr = f"\n{'-'*40}\n"
    end = f"\n{'~'*40}\n"
    s3_url = (
        "https://rikolti-data.s3.us-west-2.amazonaws.com/index.html#"
        f"{version}/data/"
    )
    opensearch_url = (
        f"{os.environ.get('OPENSEARCH_ENDPOINT', '').rstrip('/')}/"
        "_dashboards/app/dev_tools#/console"
    )
    calisphere_url = f"/collections/{collection_id}/"
    print(alias)
    if alias == 'rikolti-prd':
        calisphere_url = f"https://calisphere.org{calisphere_url}"
    else:
        calisphere_url = f"https://calisphere-stage.cdlib.org{calisphere_url}"

    print(
        f"{hr}Review indexed records at:\n {s3_url}\n\n"
        f"On opensearch at:\n {opensearch_url}\nwith query:\n"
        f"GET /{alias}/_search\n"
        f"{json.dumps(dashboard_query, indent=2)}\n\n"
        f"On calisphere at:\n {calisphere_url}\n{end}"
    )
    verbed = "published" if alias == 'rikolti-prd' else "staged"
    print(
        f"{hr}Successfully {verbed} version:\n {version}\nto the "
        f"`{alias}` index{end}"
    )

    send_event_to_sns(context, {
        'record_indexer_success': 'success', 
        'version': version, 
        'index': alias
    })


def delete_collection_task(alias, collection, context):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    try:
        deleted_versions = delete_collection(collection_id, alias)
    except Exception as e:
        # TODO: implement some rollback exception handling?
        raise e

    calisphere_url = f"/collections/{collection_id}/"
    if alias == 'rikolti-prd':
        calisphere_url = f"https://calisphere.org{calisphere_url}"
    else:
        calisphere_url = f"https://calisphere-stage.cdlib.org{calisphere_url}"

    hr = f"\n{'-'*40}\n"
    end = f"\n{'~'*40}\n"

    print(f"{hr}Review collection on calisphere at: \n {calisphere_url}\n{end}")
    verbed = "unpublished" if alias == 'rikolti-prd' else "unstaged"
    print(
        f"{hr}Successfully {verbed} versions {', '.join(deleted_versions)} "
        f"from the `{alias}` index{end}"
    )

    send_event_to_sns(context, {
        'delete_collection': 'success', 
        'deleted_versions': deleted_versions,
        'index': alias
    })


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


@task(
        task_id="create_stage_index", 
        on_failure_callback=notify_rikolti_failure,
        pool="rikolti_opensearch_pool")
def stage_collection_task(
    collection: dict, version_pages: list[str], **context):

    index_collection_task("rikolti-stg", collection, version_pages, context)


@task(
        task_id="publish_collection", 
        on_failure_callback=notify_rikolti_failure,
        pool="rikolti_opensearch_pool")
def publish_collection_task(
    collection: dict, version_pages: list[str], **context):

    index_collection_task("rikolti-prd", collection, version_pages, context)


@task(
    task_id="unpublish_collection",
    on_failure_callback=notify_rikolti_failure,
    pool="rikolti_opensearch_pool")
def unpublish_collection_task(collection: dict, **context):

    delete_collection_task("rikolti-prd", collection, context)