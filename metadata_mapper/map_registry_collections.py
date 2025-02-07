import argparse
import logging
import sys

from dataclasses import dataclass, asdict
from typing import Optional, Union

import requests

from .lambda_shepherd import MappedCollectionStatus
from .lambda_shepherd import map_collection
from .validate_mapping import create_collection_validation_csv
from rikolti.utils.versions import get_versioned_pages
from rikolti.utils.registry_client import registry_endpoint

logger = logging.getLogger(__name__)


def map_endpoint(
        url: str,
        fetched_versions: dict[str, str],
        limit: Optional[Union[int, str]]=None
    ) -> dict[int, MappedCollectionStatus]:

    response = requests.get(url=url)
    response.raise_for_status()
    total: Union[int,str] = (
        response.json().get('meta', {}).get('total_count', 1))
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


@dataclass(frozen=True)
class ValidationReportStatus:
    filepath: Optional[str] = None
    num_validation_errors: int = 0
    mapped_version: Optional[str] = None
    error: bool = False
    exception: Optional[Exception] = None

    def asdict(self):
        d = asdict(self)
        if self.exception:
            d['exception'] = str(self.exception)
        return d


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
            mapped_pages = get_versioned_pages(mapped_version)
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
