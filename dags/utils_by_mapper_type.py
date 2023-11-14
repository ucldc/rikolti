import requests
import logging
import os

from urllib.parse import urlparse

from airflow.decorators import task

from rikolti.metadata_fetcher.fetch_registry_collections import fetch_endpoint
from rikolti.metadata_mapper.map_registry_collections import map_endpoint
from rikolti.metadata_mapper.map_registry_collections import registry_endpoint
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.utils.versions import get_version, get_mapped_pages

logger = logging.getLogger("airflow.task")

@task()
def make_mapper_type_endpoint(params=None):
    if not params or not params.get('mapper_type'):
        raise ValueError("Mapper type not found in params")
    mapper_type = params.get('mapper_type')
    endpoint = (
        "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json"
        f"&mapper_type={mapper_type}&ready_for_publication=true"
    )

    print("Fetching, mapping, and validating collections listed at: ")
    print(endpoint)
    return endpoint


@task()
def fetch_endpoint_task(endpoint, params=None):
    """
    3433: [
            {
                document_count: int
                vernacular_filepath: path relative to collection id
                    ex: "3433/vernacular_version_1/data/1"
                status: 'success' or 'error'
            }
        ]
    """
    limit = params.get('limit', None) if params else None
    fetcher_job_result = fetch_endpoint(endpoint, limit, logger)
    fetched_versions = {}
    for collection_id in fetcher_job_result.keys():
        version = get_version(
            collection_id,
            fetcher_job_result[collection_id][0]['vernacular_filepath']
        )
        print(
            "Review fetched data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{version}"
        )
        fetched_versions[collection_id] = version
    return fetched_versions

@task()
def map_endpoint_task(endpoint, fetched_versions, params=None):
    limit = params.get('limit', None) if params else None
    mapper_job_results = map_endpoint(endpoint, fetched_versions, limit)
    for mapper_job in mapper_job_results:
        print(
            "Review mapped data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{mapper_job['collection_id']}/"
        )
    mapped_versions = {}
    for mapper_job_result in mapper_job_results:
        print(mapper_job_result.keys())
        mapped_version = get_version(
            mapper_job_result['collection_id'],
            mapper_job_result['mapped_page_paths'][0]
        )
        mapped_versions[mapper_job_result['collection_id']] = mapped_version

    return mapped_versions

@task()
def validate_endpoint_task(url, mapped_versions, params=None):
    limit = params.get('limit', None) if params else None

    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    if not limit:
        limit = total

    print(f">>> Validating {limit}/{total} collections described at {url}")

    csv_paths = []
    s3_paths = []
    for collection in registry_endpoint(url):
        print(f"{collection['collection_id']:<6} Validating collection")
        collection_id = collection['collection_id']
        mapped_version = mapped_versions.get(str(collection_id))
        mapped_pages = get_mapped_pages(mapped_version)
        num_rows, file_location = create_collection_validation_csv(
            collection_id, mapped_pages)
        csv_paths.append(file_location)
        validation_data_dest = os.environ.get("MAPPED_DATA", "file:///tmp")
        if validation_data_dest.startswith("s3"):
            s3_path = urlparse(f"{validation_data_dest.rstrip('/')}/{file_location}")
            s3_paths.append(f"https://{s3_path.netloc}.s3.amazonaws.com{s3_path.path}")
        print(f"Output {num_rows} rows to {file_location}")

    for s3_path in s3_paths:
        print(f"Download validation report at: {s3_path}")
        print(
            "Review collection data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{s3_path.split('/')[-3]}/"
        )

    return csv_paths

