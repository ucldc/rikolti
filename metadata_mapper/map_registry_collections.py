import argparse
import logging
import sys

from dataclasses import dataclass
from typing import Optional

import requests

from .lambda_shepherd import MappedCollectionStatus
from .lambda_shepherd import map_collection
from validate_mapping import create_collection_validation_csv
from rikolti.utils.versions import get_mapped_pages

logger = logging.getLogger(__name__)


def registry_endpoint(url):
    page = url
    while page:
        response = requests.get(url=page)
        response.raise_for_status()
        page = response.json().get('meta', {}).get('next', None)
        if page:
            page = f"https://registry.cdlib.org{page}"

        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            yield collection


def map_endpoint(url, fetched_versions, limit=None) -> dict[int, MappedCollectionStatus]:
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    # map_report_headers = (
    #     "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
    # )

    if not limit:
        limit = total
    limit = int(limit)

    print(f">>> Mapping {limit}/{total} collections described at {url}")
    # print(map_report_headers)
    map_report = {}

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']

        progress = progress + 1
        progress_bar = f"{progress}/{limit}"
        print(
            f"{collection_id:<6}: start mapping {progress_bar:<9}")

        logger.debug(
            f"{collection_id:<6}: call lambda with collection_id: {collection_id}")

        try:
            vernacular_version = fetched_versions[str(collection_id)]
            map_result = map_collection(collection_id, vernacular_version)
        except FileNotFoundError:
            print(f"{collection_id:<6}: not fetched yet", file=sys.stderr)
            continue
        except KeyError:
            print(f"{ collection_id:<6}: not fetched yet", file=sys.stderr)
            continue

        progress_bar = f"{progress}/{limit}"
        logger.debug(
            f"{collection_id:<6}: finish mapping {progress_bar}")

        map_report[collection_id] = map_result
        if limit and progress >= limit:
            break

    return map_report


@dataclass
class ValidationReportStatus:
    filepath: Optional[str] = None
    num_validation_errors: int = 0
    mapped_version: Optional[str] = None
    error: bool = False
    exception: Optional[Exception] = None


def validate_endpoint(
        url, mapped_versions, limit=None) -> dict[int, ValidationReportStatus]:
    response = requests.get(url=url)
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    progress = 0
    if not limit:
        limit = total
    limit = int(limit)

    print(f">>> Validating {limit}/{total} collections described at {url}")

    validation_reports = {}

    for collection in registry_endpoint(url):
        collection_id = collection['collection_id']
        progress = progress + 1
        print(f"{collection_id:<6} Validating collection")

        mapped_version = mapped_versions.get(str(collection_id))
        try:
            mapped_pages = get_mapped_pages(mapped_version)
        except (FileNotFoundError, ValueError) as e:
            print(f"{collection_id:<6}: not mapped yet", file=sys.stderr)
            status = ValidationReportStatus(
                mapped_version=mapped_version, error=True, exception=e
            )
            validation_reports[collection_id] = status
            continue

        try:
            num_rows, version_page = create_collection_validation_csv(
                collection_id, mapped_pages)
        except Exception as e:
            print(f"{collection_id:<6}: {e}", file=sys.stderr)
            status = ValidationReportStatus(
                mapped_version=mapped_version, error=True, exception=e
            )
            validation_reports[collection_id] = status
            continue

        validation_reports[collection_id] = ValidationReportStatus(
            filepath=version_page,
            num_validation_errors=num_rows,
            mapped_version=mapped_version
        )
        print(f"Output {num_rows} rows to {version_page}")

        if limit and progress >= limit:
            break

    return validation_reports


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run mapper for registry endpoint")
    parser.add_argument('endpoint', help='registry api endpoint')
    args = parser.parse_args(sys.argv[1:])
    map_endpoint(args.endpoint)
    sys.exit(0)
