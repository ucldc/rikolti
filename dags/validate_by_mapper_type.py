import requests
import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.metadata_fetcher.fetch_registry_collections import fetch_endpoint
from rikolti.metadata_mapper.map_registry_collections import map_endpoint
from rikolti.metadata_mapper.map_registry_collections import registry_endpoint
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.dags.shared_tasks import s3_to_localfilesystem

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
    return fetch_endpoint(endpoint, limit, logger)

@task()
def map_endpoint_task(endpoint, params=None):
    limit = params.get('limit', None) if params else None
    return map_endpoint(endpoint, limit)

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
    for collection in registry_endpoint(url):
        print(f"{collection['collection_id']:<6} Validating collection")
        num_rows, file_location = create_collection_validation_csv(
            collection['collection_id'])
        csv_paths.append(file_location)
        print(f"Output {num_rows} rows to {file_location}")

    return csv_paths

@dag(
    dag_id="validate_by_mapper_type",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'mapper_type': Param(None, description="Rikolti mapper type to harvest and validate"),
        'limit': Param(None, description="Limit number of collections to validate"),
    },
    tags=["rikolti"],
)
def validate_by_mapper_type():
    endpoint=make_mapper_type_endpoint()
    validation_reports = validate_endpoint_task(endpoint)
    (
        fetch_endpoint_task(endpoint) >>
        map_endpoint_task(endpoint) >>
        validation_reports
    )
    s3_to_localfilesystem.expand(s3_url=validation_reports)


validate_by_mapper_type()