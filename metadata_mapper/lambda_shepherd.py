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


# {"collection_id": 26098, "source_type": "nuxeo"}
# {"collection_id": 26098, "source_type": "nuxeo"}
# AWS Lambda entry point
def map_collection(collection_id):
    payload = {'collection_id': collection_id}
    collection = get_collection(collection_id)
    payload.update({'collection': collection})

    count = 0
    page_count = 0
    collection_exceptions = []

    page_list = get_vernacular_pages(collection_id)
    for page in page_list:
        payload.update({'page_filename': page})

        try:
            mapped_page = map_page(json.dumps(payload))
        except KeyError:
            print(
                f"[{collection_id}]: {collection['rikolti_mapper_type']} "
                "not yet implemented", file=sys.stderr
            )
            continue

        count += mapped_page['num_records_mapped']
        page_count += 1
        collection_exceptions.append(mapped_page.get('page_exceptions', {}))


    validate = payload.get("validate")
    if validate:
        opts = validate if isinstance(validate, dict) else {}
        validate_mapping.create_collection_validation_csv(
            collection_id,
            **opts
            )

    group_exceptions = {}
    for page_exceptions in collection_exceptions:
        for exception, couch_ids in page_exceptions.items():
            group_exceptions.setdefault(exception, []).extend(couch_ids)

    return {
        'status': 'success',
        'collection_id': collection_id,
        'missing_enrichments': check_for_missing_enrichments(collection),
        'records_mapped': count,
        'pages_mapped': page_count,
        'exceptions': group_exceptions
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('collection_id', help='collection ID from registry')
    args = parser.parse_args(sys.argv[1:])
    mapped_collection = map_collection(args.collection_id)
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