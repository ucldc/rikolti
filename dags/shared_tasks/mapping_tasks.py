import json
import os
from typing import Union, Optional

from airflow.decorators import task, task_group

from urllib.parse import urlparse

from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.utils.versions import get_version
from rikolti.utils.versions import create_mapped_version

@task()
def create_mapped_version_task(collection, vernacular_page_batches) -> str:
    """
    vernacular pages is a list of lists of the filepaths of the vernacular
    metadata relative to the collection id, ex: [
        ['3433/vernacular_metadata_2023-01-01T00:00:00/data/1'],
        ['3433/vernacular_metadata_2023-01-01T00:00:00/data/2]'
    ]
    returns the path to a new mapped version, ex:
        "3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/"
    """
    vernacular_page_batch = vernacular_page_batches[0]
    vernacular_page = vernacular_page_batch[0]
    vernacular_version = get_version(collection.get('id'), vernacular_page)
    if not vernacular_version:
        raise ValueError(
            f"Vernacular version not found in {vernacular_page}")
    mapped_data_version = create_mapped_version(vernacular_version)
    return mapped_data_version


# max_active_tis_per_dag - setting on the task to restrict how many
# instances can be running at the same time, *across all DAG runs*
@task()
def map_page_task(
    vernacular_page_batch: Union[str,list[str]],
    collection: dict,
    mapped_data_version: str):
    """
    vernacular_page_batches is a list of filepaths relative to the collection id, ex:
        [ 3433/vernacular_metadata_2023-01-01T00:00:00/data/1 ]
    or:
        [
            3433/vernacular_metadata_2023-01-01T00:00:00/data/1,
            3433/vernacular_metadata_2023-01-01T00:00:00/data/2
        ]
    mapped_data_version is a path relative to the collection id, ex:
        3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/
    returns a dictionary with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path|None: str, ex:
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    """
    collection_id = collection.get('id')
    if not collection_id or not mapped_data_version:
        return False

    mapped_pages = []
    for vernacular_page in vernacular_page_batch:
        mapped_page = map_page(
            collection_id, vernacular_page, mapped_data_version, collection)
        mapped_pages.append(mapped_page)
    return mapped_pages


@task(multiple_outputs=True)
def get_mapping_status_task(collection: dict, mapped_page_batches: list) -> dict:
    """
    mapped_pages is a list of a list of dicts with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path: str|None, ex:
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    returns a dict with the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    mapped_pages = []
    for batch in mapped_page_batches:
        mapped_pages.extend(batch)
    mapping_status = get_mapping_status(collection, mapped_pages)

    return mapping_status


@task()
def validate_collection_task(collection_id: int, mapped_metadata_pages: dict) -> str:
    """
    collection_status is a dict containing the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    parent_pages = [path for path in mapped_metadata_pages if 'children' not in path]

    num_rows, file_location = create_collection_validation_csv(
        collection_id, parent_pages)
    print(f"Output {num_rows} rows to {file_location}")

    # create a link to the file in the logs
    mapper_data_dest = os.environ.get("MAPPED_DATA", "file:///tmp")
    if mapper_data_dest.startswith("s3"):
        parsed_loc = urlparse(
            f"{mapper_data_dest.rstrip('/')}/{file_location}")
        file_location = (
            f"https://{parsed_loc.netloc}.s3.us-west-2."
            f"amazonaws.com{parsed_loc.path}"
        )

    return file_location


@task()
def get_mapped_page_filenames_task(mapped_page_batches: list[list[dict]]):
    batches = []
    for mapped_page_batch in mapped_page_batches:
        batch = [
            mapped_page['mapped_page_path'] for mapped_page in mapped_page_batch
            if mapped_page['mapped_page_path']]
        batches.append(json.dumps(batch))

    return batches


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
