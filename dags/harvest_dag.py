import json
import os

from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from typing import Optional


from rikolti.dags.shared_tasks.shared_tasks import create_vernacular_version_task
from rikolti.dags.shared_tasks.shared_tasks import fetch_collection_task
from rikolti.dags.shared_tasks.shared_tasks import get_registry_data_task
from rikolti.dags.shared_tasks.shared_tasks  import create_mapped_version_task
from rikolti.dags.shared_tasks.shared_tasks  import map_page_task
from rikolti.dags.shared_tasks.shared_tasks  import get_mapping_status_task
from rikolti.dags.shared_tasks.shared_tasks import validate_collection_task
from rikolti.dags.shared_tasks.shared_tasks import create_with_content_urls_version_task
from rikolti.dags.shared_content_harvester import ContentHarvestOperator
from rikolti.utils.versions import (
    get_child_directories, get_with_content_urls_pages,
    get_with_content_urls_page_content, get_child_pages,
    create_merged_version, put_merged_page)
from rikolti.dags.shared_tasks.shared_tasks import create_stage_index_task
from rikolti.dags.shared_tasks.shared_tasks import cleanup_failed_index_creation_task


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

@task()
def merge_children(version):
    with_content_urls_pages = get_with_content_urls_pages(version)

    # Recurse through the record's children (if any)
    child_directories = get_child_directories(version)
    if not child_directories:
        return with_content_urls_pages

    merged_version = create_merged_version(version)
    parent_pages = [page for page in with_content_urls_pages if 'children' not in page]
    merged_pages = []
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
    return merged_pages


@task()
def get_mapped_page_filenames_task(mapped_page_batches: list[list[dict]]):
    batches = []
    for mapped_page_batch in mapped_page_batches:
        batch = [
            mapped_page['mapped_page_path'] for mapped_page in mapped_page_batch
            if mapped_page['mapped_page_path']]
        batches.append(json.dumps(batch))

    return batches


@task_group(group_id='fetching')
def fetching_tasks(collection: Optional[dict] = None):
    vernacular_version = create_vernacular_version_task(
        collection=collection['registry_fetchdata'])
    fetched_page_batches = fetch_collection_task(
        collection=collection['registry_fetchdata'], 
        vernacular_version=vernacular_version)
    return fetched_page_batches


@task_group(group_id='mapping')
def mapping_tasks(
    collection: Optional[dict] = None, 
    fetched_page_batches: Optional[list[list[str]]] = None):

    mapped_data_version = create_mapped_version_task(
        collection=collection,
        vernacular_page_batches=fetched_page_batches
    )
    mapped_status_batches = (
        map_page_task
            .partial(collection=collection, mapped_data_version=mapped_data_version)
            .expand(vernacular_page_batch=fetched_page_batches)
    )

    mapping_status = get_mapping_status_task(collection, mapped_status_batches)
    validate_collection_task(collection['id'], mapping_status['mapped_page_paths'])
    mapped_page_batches = get_mapped_page_filenames_task(mapped_status_batches)

    return mapped_page_batches

@task_group(group_id='content_harvesting')
def content_harvesting_tasks(
    collection: Optional[dict] = None, 
    mapped_page_batches: Optional[list[list[str]]] = None):

    with_content_urls_version = create_with_content_urls_version_task(
        collection, mapped_page_batches)

    content_harvest_task = (
        ContentHarvestOperator
            .partial(
                task_id="content_harvest", 
                collection_id="{{ params.collection_id }}",
                with_content_urls_version=with_content_urls_version,
                mapper_type=collection['rikolti_mapper_type']
            )
            .expand(
                pages=mapped_page_batches
            )
    )
    return with_content_urls_version, content_harvest_task


@dag(
    dag_id="harvest_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to harvest"),
    },
    tags=["rikolti"],
)
def harvest():

    collection = get_registry_data_task()
    fetched_page_batches = fetching_tasks(collection)
    mapped_page_batches = mapping_tasks(collection, fetched_page_batches)
    with_content_urls_version, content_harvest_task = content_harvesting_tasks(
        collection, mapped_page_batches)
    merged_pages = merge_children(with_content_urls_version)
    merged_pages.set_upstream(content_harvest_task)
    stage_index = create_stage_index_task(collection, merged_pages)
    cleanup_failed_index_creation_task(stage_index)


harvest()