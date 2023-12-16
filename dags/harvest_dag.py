import json
import os

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


from rikolti.dags.shared_tasks import create_vernacular_version_task
from rikolti.dags.shared_tasks import fetch_collection_task
from rikolti.dags.shared_tasks import get_collection_fetchdata_task
from rikolti.dags.shared_tasks import get_collection_metadata_task
from rikolti.dags.shared_tasks  import create_mapped_version_task
from rikolti.dags.shared_tasks  import map_page_task
from rikolti.dags.shared_tasks  import get_mapping_status_task
from rikolti.dags.shared_tasks import validate_collection_task
from rikolti.dags.shared_tasks import create_with_content_urls_version_task
from rikolti.dags.shared_content_harvester import ContentHarvestOperator
from rikolti.utils.versions import (
    get_child_directories, get_with_content_urls_pages,
    get_with_content_urls_page_content, get_child_pages,
    create_merged_version, put_merged_page)
from rikolti.dags.shared_tasks import create_stage_index_task
from rikolti.dags.shared_tasks import cleanup_failed_index_creation_task


def get_child_records(version, parent_id) -> list:
    child_records = []
    children = get_child_pages(version)
    children = [page for page in children
                if (page.rsplit('/')[-1]).startswith(parent_id)]
    for child in children:
        child_records.extend(get_with_content_urls_page_content(child))
    return child_records


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
        merged_pages.append(
            put_merged_page(
                json.dumps(parent_records),
                os.path.basename(page_path),
                merged_version
            )
        )
    return merged_pages


@task()
def get_mapped_page_filenames_task(mapped_pages):
    return [mapped['mapped_page_path'] for mapped in mapped_pages]


@dag(
    dag_id="harvest_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to harvest"),
        'validate': Param(True, description="Validate mapping?")
    },
    tags=["rikolti"],
)
def harvest():

    # see TODO at get_collection_fetchdata_task
    fetchdata = get_collection_fetchdata_task()
    collection = get_collection_metadata_task()

    vernacular_version = create_vernacular_version_task(collection=fetchdata)
    fetched_pages = fetch_collection_task(
        collection=fetchdata, vernacular_version=vernacular_version)
    mapped_data_version = create_mapped_version_task(
        collection=collection,
        vernacular_pages=fetched_pages
    )
    mapped_pages = (
        map_page_task
            .partial(collection=collection, mapped_data_version=mapped_data_version)
            .expand(vernacular_page=fetched_pages)
    )

    mapping_status = get_mapping_status_task(collection, mapped_pages)
    validate_collection_task(mapping_status)
    mapped_page_paths = get_mapped_page_filenames_task(mapped_pages)

    with_content_urls_version = create_with_content_urls_version_task(
        collection, mapped_pages)

    content_harvest_task = (
        ContentHarvestOperator
            .partial(
                task_id="content_harvest", 
                collection_id="{{ params.collection_id }}",
                with_content_urls_version=with_content_urls_version,
                mapper_type=collection['rikolti_mapper_type']
            )
            .expand(
                page=mapped_page_paths
            )
    )

    merged_parent_records = merge_children(with_content_urls_version)
    content_harvest_task >> merged_parent_records

    stage_index = create_stage_index_task(collection, merged_parent_records)
    cleanup_failed_index_creation_task(stage_index)


harvest()