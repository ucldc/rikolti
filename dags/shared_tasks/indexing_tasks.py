import os
import json

from airflow.decorators import task

from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import send_event_to_sns
from rikolti.record_indexer.index_collection import index_collection
from rikolti.utils.versions import get_version

@task(task_id="create_stage_index", on_failure_callback=notify_rikolti_failure)
def update_stage_index_for_collection_task(
    collection: dict, version_pages: list[str], **context):

    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    try:
        index_collection("rikolti-stg", collection_id, version_pages)
    except Exception as e:
        # TODO: implement some rollback exception handling?
        raise e

    version = get_version(collection_id, version_pages[0])
    dashboard_query = {"query": {
        "bool": {"filter": {"terms": {"collection_url": [collection_id]}}}
    }}
    hr = f"\n{'-'*40}\n"
    end = f"\n{'~'*40}\n"
    print(
        f"{hr}Review indexed records at: \n https://rikolti-data.s3.us-west-2."
        f"amazonaws.com/index.html#{version.rstrip('/')}/data/ \n\n"
        f"Or on opensearch at: {os.environ.get('OPENSEARCH_ENDPOINT')}"
        "/_dashboards/app/dev_tools#/console with query:\n"
        f"{json.dumps(dashboard_query, indent=2)}{end}"
    )

    send_event_to_sns(context, {'record_indexer_success': 'success'})
