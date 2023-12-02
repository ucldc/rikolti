import requests
import logging
import os
import sys

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
            f"amazonaws.com/index.html#{version.rstrip('/')}/data/"
        )
        fetched_versions[collection_id] = version
    return fetched_versions

@task()
def map_endpoint_task(endpoint, fetched_versions, params=None):
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
    limit = params.get('limit', None) if params else None

    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    if not limit:
        limit = total

    print(f">>> Validating {limit}/{total} collections described at {url}")

    collections = {}
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']
        print(f"{collection_id:<6} Validating collection")

        mapped_version = mapped_versions.get(str(collection_id))
        try:
            mapped_pages = get_mapped_pages(mapped_version)
        except FileNotFoundError:
            print(f"{collection_id:<6}: not mapped yet", file=sys.stderr)
            continue

        num_rows, version_page = create_collection_validation_csv(
            collection_id, mapped_pages)

        collections[collection_id] = {
            'csv': version_page,
            'mapped_version': mapped_version,
            'data_uri': f"{data_root.rstrip('/')}/{version_page}"
        }
        print(f"Output {num_rows} rows to {version_page}")

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
                "https://{bucket}.s3.us-west-2.amazonaws.com/index.html"
                f"#{collection['mapped_version'].rstrip('/')}/data/"
            )

    return [collection['csv'] for collection in collections.values()]

