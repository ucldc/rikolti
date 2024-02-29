import logging
import math
import pprint
from typing import Optional
from dataclasses import asdict

from airflow.decorators import task, task_group

from rikolti.dags.shared_tasks.shared import batched, send_log_to_sqs
from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_fetcher.lambda_function import print_fetched_collection_report
from rikolti.metadata_fetcher.fetch_registry_collections import fetch_endpoint
from rikolti.utils.versions import create_vernacular_version

@task(task_id="create_vernacular_version",
      on_failure_callback=notify_rikolti_failure)
def create_vernacular_version_task(collection, **context) -> str:
    # returns: '3433/vernacular_metadata_v1/'
    version = create_vernacular_version(collection.get('collection_id'))
    send_log_to_sqs(context, {'vernacular_version': version})
    return version


@task(
        task_id="fetch_collection",
        on_failure_callback=notify_rikolti_failure)
def fetch_collection_task(
    collection: dict, vernacular_version: str, **context) -> list[list[str]]:
    """
    returns a list of the filepaths of the vernacular metadata relative to the
    collection id, ex: [
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/1',
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/2'
    ]
    """
    fetched_collection = fetch_collection(collection, vernacular_version)
    if not fetched_collection.filepaths:
        raise Exception(
            f"vernacular metadata not successfully fetched\n"
            f"{pprint.pprint(asdict(fetched_collection))}\n"
            f"{fetched_collection.filepaths}"
        )

    send_log_to_sqs(context, asdict(fetched_collection))

    print(f"Successfully fetched collection {collection['collection_id']}")
    print_fetched_collection_report(collection, fetched_collection)
    version = fetched_collection.version
    print(
        "Review fetched data at: https://rikolti-data.s3.us-west-2."
        f"amazonaws.com/index.html#{version.rstrip('/')}/data/"
    )

    # 1024 is the maximum number of fanout tasks allowed
    # so number of batches should never be more than 1024
    batch_size = math.ceil(len(fetched_collection.filepaths) / 1024)
    return batched(fetched_collection.filepaths, batch_size)


@task_group(group_id='fetching')
def fetching_tasks(collection: Optional[dict] = None):
    vernacular_version = create_vernacular_version_task(
        collection=collection['registry_fetchdata'])
    fetched_page_batches = fetch_collection_task(
        collection=collection['registry_fetchdata'], 
        vernacular_version=vernacular_version)
    return fetched_page_batches


logger = logging.getLogger("airflow.task")


@task(task_id="fetch_endpoint", on_failure_callback=notify_rikolti_failure)
def fetch_endpoint_task(endpoint, params=None, **context):
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
    for collection_id, fetched_collection in fetcher_job_result.items():
        version = fetched_collection.version
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

    send_log_to_sqs(context, {
        'fetched_versions': fetched_versions,
        'errored_collections': errored_collections
    })

    return fetched_versions

