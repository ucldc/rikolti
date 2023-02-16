import boto3
import json
import logging
import os
import requests
import sys
import urllib3

from typing import Union

import settings
import utilities

from mappers.validator import Validator, ValidationErrors

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def solr(**params):
    solr_url = f'{settings.SOLR_URL}/query/'
    solr_auth = {'X-Authentication-Token': settings.SOLR_API_KEY}
    query = {}
    for key, value in list(params.items()):
        key = key.replace('_', '.')
        query.update({key: value})
    res = requests.post(solr_url, headers=solr_auth, data=query, verify=False)
    res.raise_for_status()
    return json.loads(res.content.decode('utf-8'))


def get_mapped_data(collection_id: int, page_id: int) -> dict:
    if settings.DATA_SRC == 'local':
        page_path = os.sep.join([
            settings.local_path('mapped_metadata', collection_id),
            str(page_id)
        ])

        with open(page_path, "r") as metadata_file:
            return json.loads(metadata_file.read())
    elif settings.DATA_SRC == 's3':
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{collection_id}/{page}"
        s3_obj_summary = s3.Object(bucket, key).get()
        return json.loads(s3_obj_summary['Body'].read())


def get_solr_data(collection_id: int, ids: list[str]) -> list[dict]:
    collection_url = ("collection_url:\"https://registry.cdlib.org/api/v1/"
                      f"collection/{collection_id}/\"")
    harvest_id_q = " OR ".join([f"harvest_id_s:\"{f}\"" for f in ids])
    query = {
        'fq': (
            "(collection_url:\"https://registry.cdlib.org/api/v1"
            f"/collection/{collection_id}/\") AND "
            f"({harvest_id_q})"
        ),
        'rows': len(ids),
        'start': 0
    }

    response = solr(**query)
    return response.get("response", {"docs": None}).get("docs", [])


def validate_page(collection_id: int, page_id: int,
                  validator_class: Validator = Validator) -> ValidationErrors:
    validator = validator_class()

    context = {
        "collection_id": collection_id,
        "page_id": page_id
    }
    mapped_metadata = validator.generate_keys(
                        get_mapped_data(collection_id, page_id),
                        type="Rikolti",
                        context=context
                      )
    solr_data = validator.generate_keys(
                    get_solr_data(collection_id, list(mapped_metadata.keys())),
                    type="Solr",
                    context=context
                )

    if len(mapped_metadata) == 0 or len(solr_data) == 0:
        print("No data found in "
              f"{'mapped Rikolti data' if len(mapped_metadata) else 'Solr'}."
              " Aborting."
              )
        return

    intersection = set(mapped_metadata.keys()) & set(solr_data.keys())

    if len(intersection) == 0:
        print("No overlap found between Rikolti and Solr data. Aborting.")
        return

    for harvest_id in intersection:
        rikolti_record = mapped_metadata.get(harvest_id)
        solr_record = solr_data.get(harvest_id)

        validator.validate(harvest_id, rikolti_record, solr_record)

    return validator


def validate_mapped_page(rikolti_records, solr_records, query):
    collections_search = solr(**query)
    num_solr_records = collections_search['response']['numFound']
    collections_search_records = collections_search.get(
        'response', {'docs': None}).get('docs', [])
    solr_records.update(
        {r['harvest_id_s']: r for r in collections_search_records})

    page_report = []

    while len(rikolti_records):
        logging.debug(
            f"records remaining on this page: {len(rikolti_records)} "
            f"vs. records fetched from solr: {len(solr_records)}"
        )
        logging.debug(query)

        page_intersection = list(
            set(rikolti_records.keys()).intersection(solr_records.keys())
        )

        while page_intersection:
            harvest_id = page_intersection.pop(0)
            rikolti_record = rikolti_records.pop(harvest_id)
            solr_record = solr_records.pop(harvest_id)
            for field in full_fidelity_fields:
                field_name = field.get('field')
                field_type = field.get('type')
                field_validation = field.get('validation')

                rikolti_field = rikolti_record.get(field_name, None)
                solr_field = solr_record.get(field_name, None)

                if rikolti_field != solr_field:
                    page_report.append(
                        f"ERROR, field mismatch, {harvest_id}, "
                        f"{field_name}, {rikolti_field}, {solr_field}"
                    )

                field_type_check = True
                if field_type:
                    if not field_type(rikolti_field):
                        field_type_check = False
                        page_report.append(
                            f"ERROR, invalid type, {harvest_id}, "
                            f"{field_name}, {rikolti_field}"
                        )

                if field_validation and field_type_check:
                    if not field_validation(rikolti_field):
                        page_report.append(
                            f"ERROR, invalid field, {harvest_id}, "
                            f"{field_name}, {rikolti_field}"
                        )
            for field in partial_fidelity_fields:
                field_name = field
                rikolti_field = rikolti_record.get(field, None)
                solr_field = solr_record.get(field, None)
                if rikolti_field != solr_field:
                    page_report.append(
                        f"WARN, field mismatch, {harvest_id}, "
                        f"{field_name}, {rikolti_field}, {solr_field}"
                    )

        query['start'] = query['start'] + 100
        if query['start'] < num_solr_records:
            collections_search = solr(**query)
            collections_search = collections_search.get(
                'response', {'docs': None}).get('docs', [])
            solr_records.update(
                {r['harvest_id_s']: r for r in collections_search})
        else:
            logging.debug(
                f"this page intersection had {len(page_report)} errors and "
                f"{len(rikolti_records)} new rikolti records")
            for record in rikolti_records:
                page_report.append(
                    f"WARN, new rikolti record, {record}"
                )
            break

        logging.debug(f"this page intersection had {len(page_report)} errors")
    return query, solr_records, page_report


def validate_mapped_collection(payload):
    payload = json.loads(payload)
    collection_id = payload.get('collection_id')

    if settings.DATA_SRC == 'local':
        mapped_path = settings.local_path('mapped_metadata', collection_id)
        page_list = [f for f in os.listdir(mapped_path)
                     if os.path.isfile(os.path.join(mapped_path, f))]

    solr_records = {}
    query = {
        'fq': (
            "collection_url: \"https://registry.cdlib.org/api/v1"
            f"/collection/{collection_id}/\""
        ),
        'rows': 100,
        'start': 0
    }
    collection_report = ["severity, type, couch id, field_name, rikolti value, solr value"]

    for page in page_list:
        if settings.DATA_SRC == 'local':
            page_path = os.sep.join([
                settings.local_path('mapped_metadata', collection_id),
                str(page)
            ])
            page = open(page_path, "r")
            mapped_metadata = page.read()
        elif settings.DATA_SRC == 's3':
            s3 = boto3.resource('s3')
            bucket = 'rikolti'
            key = f"mapped_metadata/{collection_id}/{page}"
            s3_obj_summary = s3.Object(bucket, key).get()
            mapped_metadata = s3_obj_summary['Body'].read()

        rikolti_records = json.loads(mapped_metadata)
        rikolti_records = {
            f"{collection_id}--{r['calisphere-id']}": r
            for r in rikolti_records
        }

        print(f"[{collection_id}]: Validating page {page_path.split('/')[-1]}")
        query, solr_records, page_report = validate_mapped_page(
            rikolti_records, solr_records, query)
        collection_report = collection_report + page_report
        print(
            f"[{collection_id}]: Validated page {page_path.split('/')[-1]} "
            f"- {len(collection_report)} errors"
        )

    return collection_report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate mapped metadata against SOLR")
    parser.add_argument('collection_id', help='Collection ID')
    args = parser.parse_args(sys.argv[1:])
    collection_report = validate_mapped_collection(args.collection_id)
    print(collection_report)
