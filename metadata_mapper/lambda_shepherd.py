import json
import logging
import os
import sys

import boto3
import requests

from urllib.parse import urlparse

from . import settings, validate_mapping
from .lambda_function import map_page
from .mappers.mapper import Record


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


def get_vernacular_pages(collection_id):
    page_list = []

    if settings.DATA_SRC["STORE"] == 'file':
        vernacular_path = settings.local_path(
            'vernacular_metadata', collection_id)
        try:
            page_list = [f for f in os.listdir(vernacular_path)
                         if os.path.isfile(os.path.join(vernacular_path, f))]
            children_path = os.path.join(vernacular_path, 'children')
            if os.path.exists(children_path):
                page_list += [os.path.join('children', f)
                              for f in os.listdir(children_path)
                              if os.path.isfile(os.path.join(children_path, f))]
        except FileNotFoundError as e:
            logging.error(f"{e} - have you fetched {collection_id}?")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': (
                        f"{repr(e)} - have you fetched {collection_id}? ",
                        f"looked in dir {e.filename}"
                    )
                })
            }

    else:
        # JUST A SKETCH
        s3 = boto3.resource('s3')
        rikolti_bucket = s3.Bucket('rikolti')
        page_list = rikolti_bucket.objects.filter(
            Prefix=f'vernacular_metadata/{collection_id}')
        page_list = [p.key for p in page_list]

    return page_list


def get_mapping_stats(mapped_pages):
    count = sum([page['num_records_mapped'] for page in mapped_pages])
    page_count = len(mapped_pages)
    collection_exceptions = [page.get('page_exceptions', {}) for page in mapped_pages]

    group_exceptions = {}
    for page_exceptions in collection_exceptions:
        for exception, couch_ids in page_exceptions.items():
            group_exceptions.setdefault(exception, []).extend(couch_ids)

    return {
        'count': count,
        'page_count': page_count,
        'group_exceptions': group_exceptions
    }

def map_collection(collection_id, validate=False):
    if isinstance(validate, str):
         validate = json.loads(validate)

    collection = get_collection(collection_id)

    page_list = get_vernacular_pages(collection_id)
    mapped_pages = []
    for page in page_list:
        try:
            mapped_page = map_page(collection_id, page, collection)
            mapped_pages.append(mapped_page)
        except KeyError:
            print(
                f"[{collection_id}]: {collection['rikolti_mapper_type']} "
                "not yet implemented", file=sys.stderr
            )
            continue

    collection_stats = get_mapping_stats(mapped_pages)

    if validate:
        opts = validate if isinstance(validate, dict) else {}
        validate_mapping.create_collection_validation_csv(
            collection_id,
            **opts
            )

    return {
        'status': 'success',
        'collection_id': collection_id,
        'missing_enrichments': check_for_missing_enrichments(collection),
        'records_mapped': collection_stats.get('count'),
        'pages_mapped': collection_stats.get('page_count'),
        'exceptions': collection_stats.get('group_exceptions')
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('collection_id', help='collection ID from registry')
    parser.add_argument('--validate', help='validate mapping; may provide json opts',
        const=True, nargs='?')
    args = parser.parse_args(sys.argv[1:])
    mapped_collection = map_collection(args.collection_id, args.validate)
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