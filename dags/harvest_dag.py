import json
import os

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import notify_dag_success
from rikolti.dags.shared_tasks.shared import notify_dag_failure
from rikolti.dags.shared_tasks.shared import send_log_to_sqs
from rikolti.dags.shared_tasks.fetching_tasks import fetching_tasks
from rikolti.dags.shared_tasks.mapping_tasks  import mapping_tasks
from rikolti.dags.shared_tasks.content_harvest_tasks import content_harvesting_tasks
from rikolti.utils.versions import (
    get_child_directories, get_with_content_urls_pages,
    get_with_content_urls_page_content, get_child_pages,
    create_merged_version, put_merged_page)
from rikolti.dags.shared_tasks.indexing_tasks import create_stage_index_task


def get_child_records(version, parent_id) -> list:
    child_records = []
    children = get_child_pages(version)
    children = [page for page in children
                if (page.rsplit('/')[-1]).startswith(parent_id)]
    for child in children:
        child_records.extend(get_with_content_urls_page_content(child))
    return child_records

def get_child_thumbnail(child_records):
    for child in child_records:
        if child.get("thumbnail"):
            return child.get("thumbnail")

@task(task_id="merge_any_child_records", 
      on_failure_callback=notify_rikolti_failure)
def merge_any_child_records_task(version, **context):
    with_content_urls_pages = get_with_content_urls_pages(version)

    # Recurse through the record's children (if any)
    child_directories = get_child_directories(version)
    if not child_directories:
        send_log_to_sqs(context, {'records_with_children': None})
        return with_content_urls_pages

    merged_version = create_merged_version(version)
    parent_pages = [page for page in with_content_urls_pages 
                    if 'children' not in page]
    merged_pages = []
    child_count_by_record = {}
    for page_path in parent_pages:
        parent_records = get_with_content_urls_page_content(page_path)
        for record in parent_records:
            calisphere_id = record['calisphere-id']
            child_records = get_child_records(version, calisphere_id)
            if not child_records:
                continue
            print(
                f"{page_path}: {len(child_records)} children found of "
                f"record {calisphere_id}."
            )
            child_count_by_record[calisphere_id] = len(child_records)
            record['children'] = child_records
            # if the parent doesn't have a thumbnail, grab one from children
            if not record.get('thumbnail'):
                child_thumbnail = get_child_thumbnail(child_records)
                if child_thumbnail:
                    record['thumbnail'] = child_thumbnail
        merged_pages.append(
            put_merged_page(
                json.dumps(parent_records),
                os.path.basename(page_path),
                merged_version
            )
        )
    send_log_to_sqs(context, {'records_with_children': child_count_by_record})
    return merged_pages


@dag(
    dag_id="harvest_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to harvest"),
    },
    tags=["rikolti"],
    on_failure_callback=notify_dag_failure,
    on_success_callback=notify_dag_success,
)
def harvest():

    collection = get_registry_data_task()
    fetched_page_batches = fetching_tasks(collection)
    mapped_page_batches = mapping_tasks(collection, fetched_page_batches)
    with_content_urls_version, content_harvest_task = content_harvesting_tasks(
        collection, mapped_page_batches)
    merged_pages = merge_any_child_records_task(with_content_urls_version)
    merged_pages.set_upstream(content_harvest_task)
    stage_index = create_stage_index_task(collection, merged_pages)  # noqa F841


harvest()