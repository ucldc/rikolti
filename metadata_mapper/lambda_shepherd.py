import json
import sys

import requests

from urllib.parse import urlparse

from . import validate_mapping
from .lambda_function import map_page
from .mappers.mapper import Record
from rikolti.utils.rikolti_storage import list_pages, create_mapped_version, get_most_recent_vernacular_version


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
        collection.get('rikolti__enrichments')
    )
    for e_url in collection_enrichments:
        e_path = urlparse(e_url).path
        e_func_name = e_path.strip('/').replace('-', '_')
        if e_func_name not in dir(Record):
            not_yet_implemented.append(e_func_name)

    return not_yet_implemented


def get_vernacular_pages(collection_id, vernacular_version):
    try:
        page_list = list_pages(vernacular_version, recursive=True)
    except FileNotFoundError as e:
        print(
            f"{e} - have you fetched {collection_id}? "
            f"looked in dir {e.filename} for vernacular pages"
        )
        raise(e)

    # TODO: split page_list into pages and children?
    return page_list


def get_mapping_status(collection, mapped_pages):
    count = sum([page['num_records_mapped'] for page in mapped_pages])
    page_count = len(mapped_pages)
    collection_exceptions = [page.get('page_exceptions', {}) for page in mapped_pages]

    group_exceptions = {}
    for page_exceptions in collection_exceptions:
        for exception, couch_ids in page_exceptions.items():
            group_exceptions.setdefault(exception, []).extend(couch_ids)

    return {
        'status': 'success',
        'collection_id': collection.get('id'),
        'pre_mapping': collection.get('rikolti__pre_mapping'),
        'enrichments': collection.get('rikolti__enrichments'),
        'missing_enrichments': check_for_missing_enrichments(collection),
        'count': count,
        'page_count': page_count,
        'group_exceptions': group_exceptions,
        'mapped_page_paths': [page['mapped_page_path'] for page in mapped_pages],
    }

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
    page_list = get_vernacular_pages(collection_id, vernacular_version)

    mapped_data_version = create_mapped_version(collection_id, page_list[0])
    mapped_pages = []
    for page in page_list:
        try:
            mapped_page = map_page(
                collection_id, page, mapped_data_version, collection)
            mapped_pages.append(mapped_page)
        except KeyError:
            print(
                f"[{collection_id}]: {collection['rikolti_mapper_type']} "
                "not yet implemented", file=sys.stderr
            )
            continue

    collection_stats = get_mapping_status(collection, mapped_pages)
    mapped_page_paths = [page['mapped_page_path'] for page in mapped_pages]

    if validate:
        opts = validate if isinstance(validate, dict) else {}
        num_rows, file_location = (
            validate_mapping.create_collection_validation_csv(
                collection_id,
                mapped_page_paths,
                **opts
            )
        )
        print(f"Output {num_rows} rows to {file_location}")

    return collection_stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('collection_id', help='collection ID from registry')
    parser.add_argument('--validate', help='validate mapping; may provide json opts',
        const=True, nargs='?')
    parser.add_argument('vernacular_version', help='URI to a folder of vernacular pages to map')
    args = parser.parse_args(sys.argv[1:])
    mapped_collection = map_collection(args.collection_id, args.vernacular_version, args.validate)
    missing_enrichments = mapped_collection.get('missing_enrichments')
    if len(missing_enrichments) > 0:
        print(
            f"{args.collection_id}, missing enrichments, ",
            f"ALL, -, -, {missing_enrichments}"
        )

    exceptions = mapped_collection.get('exceptions', {})
    for report, couch_ids in exceptions.items():
        print(f"{len(couch_ids)} records report enrichments errors: {report}")
        print(f"check the following ids for issues: {couch_ids}")