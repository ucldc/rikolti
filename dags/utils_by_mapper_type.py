import requests
import logging
import sys

from urllib.parse import urlparse

from airflow.decorators import task

from rikolti.metadata_fetcher.fetch_registry_collections import fetch_endpoint
from rikolti.metadata_mapper.map_registry_collections import map_endpoint
from rikolti.metadata_mapper.map_registry_collections import registry_endpoint
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv

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
    limit = params.get('limit', None) if params else None
    fetcher_job_result = fetch_endpoint(endpoint, limit, logger)
    for collection_id in fetcher_job_result.keys():
        print(
            "Review fetched data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{collection_id}/"
        )
    return fetcher_job_result

@task()
def map_endpoint_task(endpoint, params=None):
    limit = params.get('limit', None) if params else None
    mapper_job_results = map_endpoint(endpoint, limit)
    for mapper_job in mapper_job_results:
        print(
            "Review mapped data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{mapper_job['collection_id']}/"
        )
    return mapper_job_results

@task()
def validate_endpoint_task(url, params=None):
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
        try:
            num_rows, file_location = create_collection_validation_csv(
                collection['collection_id'])
        except FileNotFoundError:
            print(f"{collection['collection_id']:<6}: not mapped yet", file=sys.stderr)
            continue
        csv_paths.append(file_location)
        if file_location.startswith('s3://'):
            s3_path = urlparse(file_location)
            s3_paths.append(f"https://{s3_path.netloc}.s3.amazonaws.com{s3_path.path}")
        print(f"Output {num_rows} rows to {file_location}")

    for s3_path in s3_paths:
        print(f"Download validation report at: {s3_path}")
        print(
            "Review collection data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{s3_path.split('/')[-3]}/"
        )

    return csv_paths

