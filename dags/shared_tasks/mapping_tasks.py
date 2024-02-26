import json
import os
import requests
import sys
import traceback

from typing import Union, Optional

from airflow.decorators import task, task_group

from urllib.parse import urlparse

from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.map_registry_collections import map_endpoint
from rikolti.metadata_mapper.map_registry_collections import registry_endpoint
from rikolti.metadata_mapper.map_registry_collections import print_map_status
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.utils.versions import create_mapped_version
from rikolti.utils.versions import get_mapped_pages
from rikolti.utils.versions import get_version

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


@task()
def get_mapping_status_task(collection: dict, mapped_status_batches: list) -> list[str]:
    """
    mapped_status_batches is a list of a list of dicts with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path: str|None, ex:
            3433/vernacular_metadata_v1/mapped_metadata_v1/1.jsonl
    returns a list of strings, ex:
        [
            "[3433/vernacular_metadata_v1/mapped_metadata_v1/1.jsonl]",
            "[3433/vernacular_metadata_v1/mapped_metadata_v1/2.jsonl]",
            "[3433/vernacular_metadata_v1/mapped_metadata_v1/3.jsonl]"
        ]
    """
    mapped_statuses = []
    mapped_page_batches = []
    for mapped_status_batch in mapped_status_batches:
        mapped_statuses.extend(mapped_status_batch)
        mapped_page_batch = [
            mapped_status['mapped_page_path'] for mapped_status in mapped_status_batch
            if mapped_status['mapped_page_path']]
        mapped_page_batches.append(json.dumps(mapped_page_batch))

    mapping_status = get_mapping_status(collection, mapped_statuses)
    print_map_status(collection, mapping_status)

    mapped_version = get_version(
        collection['id'], mapping_status['mapped_page_paths'][0])
    print(
        "Review mapped data at: https://rikolti-data.s3.us-west-2."
        f"amazonaws.com/index.html#{mapped_version.rstrip('/')}/data/"
    )

    return mapped_page_batches


@task()
def validate_collection_task(collection_id: int, mapped_page_batches: list[str]) -> str:
    """
    mapped_page_batches is a list of str representations of lists of mapped
    page paths, ex:
    [
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/1.jsonl]",
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/2.jsonl]",
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/3.jsonl]"
    ]
    """
    mapped_metadata_pages = []
    for mapped_page_batch in mapped_page_batches:
        mapped_metadata_pages.extend(json.loads(mapped_page_batch))

    parent_pages = [path for path in mapped_metadata_pages if 'children' not in path]

    num_rows, version_page = create_collection_validation_csv(
        collection_id, parent_pages)

    print(f"Output {num_rows} rows to {version_page}")

    # create a link to the file in the logs
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")
    if data_root.startswith('s3'):
        s3_path = urlparse(f"{data_root.rstrip('/')}/{version_page}")
        bucket = s3_path.netloc
        print(
            "Download validation report at: "
            f"https://{bucket}.s3.amazonaws.com{s3_path.path}"
        )
        mapped_version = get_version(collection_id, parent_pages[0])
        print(
            f"Review collection data at: "
            f"https://{bucket}.s3.us-west-2.amazonaws.com/index.html"
            f"#{mapped_version.rstrip('/')}/data/"
        )

    return version_page


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

    mapped_page_batches = get_mapping_status_task(collection, mapped_status_batches)
    validate_collection_task(collection['id'], mapped_page_batches)

    return mapped_page_batches


@task()
def map_endpoint_task(endpoint, fetched_versions, params=None):
    if not fetched_versions:
        raise ValueError("No fetched versions provided to map")

    limit = params.get('limit', None) if params else None
    mapper_job_results = map_endpoint(endpoint, fetched_versions, limit)
    mapped_versions = {}
    for mapper_job_result in mapper_job_results:
        mapped_version = get_version(
            mapper_job_result['collection_id'],
            mapper_job_result['mapped_page_paths'][0]
        )
        print(
            "Review mapped data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{mapped_version.rstrip('/')}/data/"
        )
        mapped_versions[mapper_job_result['collection_id']] = mapped_version

    return mapped_versions


@task()
def validate_endpoint_task(url, mapped_versions, params=None):
    if not mapped_versions:
        raise ValueError("No mapped versions provided to validate")

    limit = params.get('limit', None) if params else None

    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    if not limit:
        limit = total
    limit = int(limit)

    print(f">>> Validating {limit}/{total} collections described at {url}")

    collections = {}
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")

    errored_collections = {}

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']
        progress = progress + 1
        print(f"{collection_id:<6} Validating collection")

        mapped_version = mapped_versions.get(str(collection_id))
        try:
            mapped_pages = get_mapped_pages(mapped_version)
        except (FileNotFoundError, ValueError) as e:
            print(f"{collection_id:<6}: not mapped yet", file=sys.stderr)
            errored_collections[collection_id] = e
            continue

        try:
            num_rows, version_page = create_collection_validation_csv(
                collection_id, mapped_pages)
        except Exception as e:
            print(f"{collection_id:<6}: {e}", file=sys.stderr)
            errored_collections[collection_id] = e
            continue

        collections[collection_id] = {
            'csv': version_page,
            'mapped_version': mapped_version,
            'data_uri': f"{data_root.rstrip('/')}/{version_page}"
        }
        print(f"Output {num_rows} rows to {version_page}")

        if limit and progress >= limit:
            break

    if data_root.startswith('s3'):
        for collection in collections.values():
            s3_path = urlparse(collection['data_uri'])
            bucket = s3_path.netloc
            print(
                "Download validation report at: "
                f"https://{bucket}.s3.amazonaws.com{s3_path.path}"
            )
            print(
                f"Review collection data at: "
                f"https://{bucket}.s3.us-west-2.amazonaws.com/index.html"
                f"#{collection['mapped_version'].rstrip('/')}/data/"
            )

    if len(errored_collections) > 0:
        print("-" * 60, file=sys.stderr)
        header = ' Validation Errors '
        print(f"{header:-^60}", file=sys.stderr)
        print("-" * 60, file=sys.stderr)
    for collection_id, e in errored_collections.items():
        collection_error_header = f" Collection {collection_id}: {e} "
        print(f"{collection_error_header:>^60}", file=sys.stderr)
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
        print("<"*60, file=sys.stderr)
    if len(errored_collections) > 0:
        print("*" * 60)
        print(f"{len(errored_collections)} collections encountered validation errors")
        print(f"please validate manually: {list(errored_collections.keys())}")
        print("*" * 60)

    if not len(collections):
        print("-", file=sys.stderr)
        raise ValueError("No collections successfully validated, exiting.")

    return [collection['csv'] for collection in collections.values()]

