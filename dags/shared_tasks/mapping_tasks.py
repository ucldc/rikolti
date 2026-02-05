import json
import math
import os
import sys
import traceback

from dataclasses import asdict
from itertools import chain
from typing import Dict, Union, Optional

from airflow.decorators import task, task_group

from urllib.parse import urlparse

from rikolti.dags.shared_tasks.shared import batched
from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import send_event_to_sns
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_function import MappedPageStatus
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.lambda_shepherd import print_map_status
from rikolti.metadata_mapper.map_registry_collections import map_endpoint
from rikolti.metadata_mapper.map_registry_collections import validate_endpoint
from rikolti.utils.versions import create_mapped_version
from rikolti.utils.versions import get_version
from rikolti.dags.shared_tasks.diffs import create_qa_reports
from rikolti.dags.shared_tasks.diffs import DiffReportStatus

@task(task_id="create_mapped_version",
      on_failure_callback=notify_rikolti_failure)
def create_mapped_version_task(
    collection, vernacular_page_batches, **context) -> str:
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
    send_event_to_sns(context, mapped_data_version)
    return mapped_data_version


# max_active_tis_per_dag - setting on the task to restrict how many
# instances can be running at the same time, *across all DAG runs*
@task(task_id="map_page",
      on_failure_callback=notify_rikolti_failure)
def map_page_task(
    vernacular_page_batch: Union[str,list[str]],
    collection: dict,
    mapped_data_version: str,
    **context) -> list[dict]:
    """
    vernacular_page_batch: a list of vernacular page filepaths, ex:
        [
            3433/vernacular_metadata_2023-01-01T00:00:00/data/1,
            3433/vernacular_metadata_2023-01-01T00:00:00/data/2
        ]
    collection: a dict representation of a collection
    mapped_data_version: a version path, ex:
        3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/

    returns:
        a list of MappedPageStatus objects as dictionaries
    """
    collection_id = collection.get('id')
    if not collection_id or not mapped_data_version:
        return []
    mapped_page_statuses = []
    for vernacular_page in vernacular_page_batch:
        mapped_page_status = map_page(
            collection_id, vernacular_page, mapped_data_version, collection)
        mapped_page_statuses.append(asdict(mapped_page_status))
    send_event_to_sns(context, mapped_page_statuses)
    return mapped_page_statuses


@task(task_id="get_mapping_status",
      on_failure_callback=notify_rikolti_failure)
def get_mapping_status_task(
    collection: dict, mapped_status_batches: list, **context) -> list[str]:
    """
    collection:
        a dict representation of a collection
    mapped_status_batches:
        a list of a batch of MappedPageStatus objects as dictionaries

    returns:
        a list of str representations of a batch (list) of mapped page paths
        ex:
            [
                "[3433/vernacular_metadata_v1/mapped_metadata_v1/1.jsonl]",
                "[3433/vernacular_metadata_v1/mapped_metadata_v1/2.jsonl]",
                "[3433/vernacular_metadata_v1/mapped_metadata_v1/3.jsonl]"
            ]
    """
    mapped_page_statuses = [MappedPageStatus(**page_status) for page_status in
                            chain.from_iterable(mapped_status_batches)]

    mapped_collection = get_mapping_status(collection, mapped_page_statuses)
    print_map_status(collection, mapped_collection)
    send_event_to_sns(context, asdict(mapped_collection))

    print(
        "Review mapped data at: https://rikolti-data.s3.us-west-2."
        "amazonaws.com/index.html#"
        f"{mapped_collection.version.rstrip('/')}/data/"
    )

    batch_size = math.ceil(len(mapped_collection.filepaths) / 50)
    batched_mapped_pages = batched(mapped_collection.filepaths, batch_size)
    return [json.dumps(batch) for batch in batched_mapped_pages]


def print_s3_link(version_page, mapped_version):
    # create a link to the file in the logs
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp").rstrip('/')
    if data_root.startswith('s3'):
        s3_path = urlparse(f"{data_root}/{version_page}")
        bucket = s3_path.netloc
        print(
            "Download report at: "
            f"https://{bucket}.s3.amazonaws.com{s3_path.path}"
        )
        print(
            "Review report at: "
            f"https://{bucket}.s3.us-west-2.amazonaws.com/index.html"
            f"#{s3_path.path.lstrip('/')}"
        )
        print(
            f"Review collection data at: "
            f"https://{bucket}.s3.us-west-2.amazonaws.com/index.html"
            f"#{mapped_version.rstrip('/')}/data/"
        )


@task(task_id="validate_collection",
      on_failure_callback=notify_rikolti_failure)
def validate_collection_task(
    collection_id: int, mapped_page_batches: list[str], **context) -> Dict[str, str]:
    """
    mapped_page_batches is a list of str representations of lists of all the
    mapped page paths, ex:
    [
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/1.jsonl]",
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/2.jsonl]",
        "[3433/vernacular_metadata_v1/mapped_metadata_v1/3.jsonl]"
    ]
    """
    mapped_page_batches = [json.loads(batch) for batch in mapped_page_batches]
    mapped_pages = list(chain.from_iterable(mapped_page_batches))
    mapped_pages = [path for path in mapped_pages if 'children' not in path]

    summary_report, detail_report = create_qa_reports(collection_id, mapped_pages)

    status = DiffReportStatus(
        summary_report_filepath=summary_report,
        detail_report_filepath=detail_report,
        mapped_version=get_version(collection_id, mapped_pages[0])
    )
    print(f"Output summary report to {summary_report}")
    print_s3_link(summary_report, get_version(collection_id, mapped_pages[0]))

    print(f"Output detail report to {detail_report}")
    print_s3_link(detail_report, get_version(collection_id, mapped_pages[0]))

    send_event_to_sns(context, asdict(status))
    return {"summary_report": summary_report, "detail_report": detail_report}


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


@task(task_id="map_endpoint", on_failure_callback=notify_rikolti_failure)
def map_endpoint_task(endpoint, fetched_versions, params=None, **context):
    if not fetched_versions:
        raise ValueError("No fetched versions provided to map")

    limit = params.get('limit', None) if params else None
    mapped_collections = map_endpoint(endpoint, fetched_versions, limit)
    mapped_versions = {}
    for collection_id, mapped_collection_status in mapped_collections.items():
        print(
            f"Review mapped data at: {mapped_collection_status.data_link}"
        )
        mapped_versions[collection_id] = mapped_collection_status.version

    send_event_to_sns(context,
                      {k: v.asdict() for k, v in mapped_collections.items()})
    return mapped_versions


@task(task_id="validate_endpoint", on_failure_callback=notify_rikolti_failure)
def validate_endpoint_task(url, mapped_versions, params=None, **context):
    if not mapped_versions:
        raise ValueError("No mapped versions provided to validate")

    limit = params.get('limit', None) if params else None

    validations = validate_endpoint(url, mapped_versions, limit)

    for validation_status in validations.values():
        if not validation_status.error:
            print_s3_link(
                validation_status.filepath, validation_status.mapped_version)

    errored_collections = {
        cid: status for cid, status in validations.items()
        if status.error and status.exception
    }
    if len(errored_collections) > 0:
        print("-" * 60, file=sys.stderr)
        header = ' Validation Errors '
        print(f"{header:-^60}", file=sys.stderr)
        print("-" * 60, file=sys.stderr)
    for collection_id, status in errored_collections.items():
        e = status.exception
        if not e:
            continue
        collection_error_header = f" Collection {collection_id}: {e} "
        print(f"{collection_error_header:>^60}", file=sys.stderr)
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
        print("<"*60, file=sys.stderr)
    if len(errored_collections) > 0:
        print("*" * 60)
        print(f"{len(errored_collections)} collections encountered validation errors")
        print(f"please validate manually: {list(errored_collections.keys())}")
        print("*" * 60)

    if len(errored_collections) == len(validations):
        print("-", file=sys.stderr)
        raise ValueError("No collections successfully validated, exiting.")

    send_event_to_sns(context,
                      {k: v.asdict() for k, v in validations.items()})
    return [
        validation_status.filepath
        for validation_status in validations.values() 
        if validation_status.filepath
    ]

