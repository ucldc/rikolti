import json
import sys

import requests

from dataclasses import dataclass
from urllib.parse import urlparse

from . import validate_mapping
from .lambda_function import map_page, MappedPageStatus
from .mappers.mapper import Record
from rikolti.utils.versions import (
    get_most_recent_vernacular_version, get_versioned_pages,
    get_version, create_mapped_version
)


def get_collection(collection_id):
    collection = requests.get(
        f'https://registry.cdlib.org/api/v1/'
        f'rikolticollection/{collection_id}/?format=json'
    ).json()
    return collection


def check_for_missing_enrichments(collection):
    """Check for missing enrichments - used for development but
    could likely be removed in production?"""

    not_yet_implemented = []
    collection_enrichments = (
        (collection.get('rikolti__pre_mapping') or []) +
        (collection.get('rikolti__enrichments') or [])
    )
    for e_url in collection_enrichments:
        e_path = urlparse(e_url).path
        e_func_name = e_path.strip('/').replace('-', '_')
        if e_func_name not in dir(Record):
            not_yet_implemented.append(e_func_name)

    return not_yet_implemented


@dataclass
class MappedCollectionStatus:
    status: str
    num_mapped_records: int
    num_mapped_pages: int
    missing_enrichments: list[str]
    exceptions: dict
    filepaths: list[str]
    version: str
    data_link: str


def get_mapping_status(
        collection,
        mapped_page_statuses: list[MappedPageStatus]) -> MappedCollectionStatus:
    """
    mapped_pages is a list of MappedPageStatus objects
    returns a MappedCollectionStatus
    """
    num_mapped_records = sum([s.num_mapped_records for s in mapped_page_statuses])
    exceptions = [s.exceptions for s in mapped_page_statuses]

    group_exceptions = {}
    for page_exceptions in exceptions:
        for exception, couch_ids in page_exceptions.items():
            group_exceptions.setdefault(exception, []).extend(couch_ids)

    version = None
    for mapped_page_status in mapped_page_statuses:
        if mapped_page_status.mapped_page_path:
            version = get_version(
                collection['collection_id'],
                mapped_page_status.mapped_page_path
            )
            break

    if not version:
        raise ValueError(
            f"Could not determine version from {mapped_page_statuses}"
        )

    return MappedCollectionStatus(
        'success',
        num_mapped_records,
        len(mapped_page_statuses),
        check_for_missing_enrichments(collection),
        group_exceptions,
        [page.mapped_page_path for page in mapped_page_statuses
            if page.mapped_page_path],
        version,
        (
            "https://rikolti-data.s3.us-west-2.amazonaws.com/index.html#"
            f"{version.rstrip('/')}/data/"
        )
    )


def print_map_status(collection, map_result: MappedCollectionStatus):
    collection_id = collection['collection_id']
    pre_mapping = collection.get('rikolti__pre_mapping', [])
    enrichments = collection.get('rikolti__enrichments', [])

    if pre_mapping and len(pre_mapping) > 0:
        print(
            f"{collection_id:<6}: {'pre-mapping enrichments':<24}: "
            f"\"{pre_mapping}\""
        )

    if enrichments and len(enrichments) > 0:
        print(
            f"{collection_id:<6}, {'post-mapping enrichments':<24}: "
            f"\"{enrichments}\""
        )

    missing_enrichments = map_result.missing_enrichments or []
    if len(missing_enrichments) > 0:
        print(
            f"{collection_id:<6}, {'missing enrichments':<24}: "
            f"\"{missing_enrichments}\""
        )
    exceptions = map_result.exceptions or {}
    if len(exceptions) > 0:
        for exception, couch_ids in exceptions.items():
            print(
                f"{collection_id:<6}, {'enrichment errors':<24}: "
                f"{len(couch_ids)} items: \"{exception}\""
            )
            print(
                f"{collection_id:<6}, {'enrichment error records':24}: "
                f"{len(couch_ids)} items: \"{couch_ids}\""
            )

    # "Collection ID, Status, Extent, Solr Count, Diff Count, Message"
    success = 'success' if map_result.status == 'success' else 'error'

    extent = map_result.num_mapped_records
    diff = extent - collection['solr_count']
    diff_items_label = ""
    if diff > 0:
        diff_items_label = 'new items'
    elif diff < 0:
        diff_items_label = 'lost items'
    else:
        diff_items_label = 'same extent'

    map_report_row = (
        f"{collection_id:<6}, {success:9}, {extent:>6} items, "
        f"{collection['solr_count']:>6} solr items, "
        f"{str(diff) + ' ' + diff_items_label + ',':>16} "
        f"solr count last updated: {collection['solr_last_updated']}"
    )
    print(map_report_row)


def map_collection(collection_id, vernacular_version=None, validate=False):
    # This is a functional duplicate of rikolti.d*gs.mapper_d*g.mapper_d*g

    # Within an airflow runtime context, we take advantage of airflow's dynamic
    # task mapping to fan out all calls to map_page. 
    # Outside the airflow runtime context, on the command line for example, 
    # map_collection performs manual "fan out" in the for loop below. 

    # Any changes to map_collection should be carefully considered, duplicated
    # to mapper_d*g, and tested in both contexts. 

    if isinstance(validate, str):
         validate = json.loads(validate)

    collection = get_collection(collection_id)

    if not vernacular_version:
        vernacular_version = get_most_recent_vernacular_version(collection_id)
    page_list = get_versioned_pages(vernacular_version)
    # TODO: split page_list into pages and children?

    vernacular_version = get_version(collection_id, page_list[0])
    mapped_data_version = create_mapped_version(vernacular_version)
    mapped_pages = []
    for page in page_list:
        try:
            mapped_page = map_page(
                collection_id, page, mapped_data_version, collection)
            mapped_pages.append(mapped_page)
        except KeyError as e:
            print(
                f"[{collection_id}]: {collection['rikolti_mapper_type']} "
                f"not yet implemented, {e}", file=sys.stderr
            )
            continue

    mapped_collection = get_mapping_status(collection, mapped_pages)
    print_map_status(collection, mapped_collection)

    if validate:
        opts = validate if isinstance(validate, dict) else {}
        num_rows, file_location = (
            validate_mapping.create_collection_validation_csv(
                collection_id,
                mapped_collection.filepaths,
                **opts
            )
        )
        print(f"Output {num_rows} rows to {file_location}")

    return mapped_collection


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('collection_id', help='collection ID from registry')
    parser.add_argument('--validate', help='validate mapping; may provide json opts',
        const=True, nargs='?')
    parser.add_argument('vernacular_version', help='relative path describing a vernacular version, ex: 3433/vernacular_data_version_1/')
    args = parser.parse_args(sys.argv[1:])
    mapped_collection = map_collection(args.collection_id, args.vernacular_version, args.validate)
    missing_enrichments = mapped_collection.missing_enrichments
    if len(missing_enrichments) > 0:
        print(
            f"{args.collection_id}, missing enrichments, ",
            f"ALL, -, -, {missing_enrichments}"
        )

    exceptions = mapped_collection.exceptions
    for report, couch_ids in exceptions.items():
        print(f"{len(couch_ids)} records report enrichments errors: {report}")
        print(f"check the following ids for issues: {couch_ids}")