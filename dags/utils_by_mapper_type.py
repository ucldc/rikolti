import requests
import logging
import os
import sys
import traceback

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
    if not params:
        raise ValueError("No parameters provided")

    arg_keys = ['mapper_type', 'rikolti_mapper_type', 'registry_api_queryset']
    args = {key: params.get(key) for key in arg_keys if params.get(key)}
    if not any(args.values()):
        raise ValueError("Endpoint data not found in params, please provide "
                         "either a mapper_type, a rikolti_mapper_type, or a "
                         "registry_api_queryset")

    which_arg = list(args.keys())
    if len(which_arg) > 1:
        raise ValueError("Please provide only one of mapper_type, "
                         "rikolti_mapper_type, or registry_api_queryset")

    which_arg = which_arg[0]
    if which_arg == 'mapper_type':
        mapper_type = params.get('mapper_type')
        endpoint = (
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json"
            f"&mapper_type={mapper_type}&ready_for_publication=true"
        )
    elif which_arg == 'rikolti_mapper_type':
        rikolti_mapper_type = params.get('rikolti_mapper_type')
        endpoint = (
            "https://registry.cdlib.org/api/v1/rikoltifetcher/?format=json"
            f"&rikolti_mapper_type={rikolti_mapper_type}"
            "&ready_for_publication=true"
        )
    elif which_arg == 'registry_api_queryset':
        endpoint = params.get('registry_api_queryset')
        # TODO: validate endpoint is a valid registry endpoint describing
        # a queryset of collections
    else:
        raise ValueError(
            "Please provide a mapper_type, rikolti_mapper_type, or endpoint")

    offset = params.get('offset')
    if offset:
        endpoint = endpoint + f"&offset={offset}"

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
    errored_collections = {}
    for collection_id, fetch_job in fetcher_job_result.items():
        if isinstance(fetch_job, dict) and 'error' in fetch_job:
            errored_collections[collection_id] = fetch_job
            continue
        if not isinstance(fetch_job, list):
            errored_collections[collection_id] = fetch_job
            continue
        if not fetch_job[0].get('vernacular_filepath'):
            errored_collections[collection_id] = fetch_job
            continue
        version = get_version(
            collection_id,
            fetch_job[0]['vernacular_filepath']
        )
        print(
            "Review fetched data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{version.rstrip('/')}/data/"
        )
        fetched_versions[collection_id] = version
    if errored_collections:
        print(
            f"{len(errored_collections)} encountered an error when "
            f"fetching: {errored_collections.keys()}"
        )
        print(errored_collections)

    if not fetched_versions:
        raise ValueError("No collections successfully fetched, exiting.")

    return fetched_versions

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

