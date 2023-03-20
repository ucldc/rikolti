import boto3
import json
import logging
import os
import requests
import sys
import urllib3

from datetime import datetime
from pathlib import Path
from typing import Type, Union

import settings
import utilities

from map_registry_collections import lookup

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
    return utilities.read_mapped_metadata(collection_id, page_id)


def get_solr_data(collection_id: int, ids: list[str]) -> list[dict]:
    collection_url = ("collection_url:\"https://registry.cdlib.org/api/v1/"
                      f"collection/{collection_id}/\"")
    harvest_id_q = " OR ".join([f"harvest_id_s:\"{f}\"" for f in ids])
    query = {
        'fq': f"{collection_url} AND ({harvest_id_q})",
        'rows': len(ids),
        'start': 0
    }

    response = solr(**query)
    return response.get("response", {"docs": None}).get("docs", [])


def get_validator_class(collection_id: int) -> Type[Validator]:
    url = ("https://registry.cdlib.org/api/v1/rikoltimapper/"
           f"{collection_id}/?format=json")

    try:
        response = requests.get(url=url)
        response.raise_for_status()
        collection_data = response.json()
    except requests.exceptions.HTTPError as err:
        print(
            f"[Collection {collection_id}]: "
            f"[{url}]"
            f"{err}; A valid collection id is required for validation"
        )
        return

    mapper = collection_data.get("rikolti_mapper_type")
    mapped_mapper = lookup.get(mapper, mapper)
    vernacular = utilities.import_vernacular_reader(mapped_mapper)

    return vernacular.record_cls.validator


def validate_collection(collection_id: int,
                        validator_class: Type[Validator] = None,
                        validator: Validator = None
                        ) -> Validator:
    """
    Validates all pages of a collection of mapped data.

    Parameters:
        collection_id: int
            The collection ID
        validator_class: Type[Validator] (default: None)
            The validator class to use. Can be derived if not provided.
        validator: Validator (default: None)
            A validator instance to use. Can be derived if not provided.

    Returns: Validator
        The validator containing errors
    """
    if not validator_class and not validator:
        validator_class = get_validator_class(collection_id)

    if not validator:
        validator = validator_class()

    for page_id in utilities.get_files("mapped_metadata", collection_id):
        validate_page(collection_id, page_id, validator)

    return validator


def validate_page(collection_id: int, page_id: int,
                  validator: Validator) -> Validator:
    """
    Validates a provided page of a provided collection of mapped data.

    Parameters:
        collection_id: int
            The collection ID
        page_id: int
            The page number within the collection
        validator: Validator
            The validator instance to use

    Returns Validator
        The validator containing validation errors
    """
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

    all_keys = set(mapped_metadata.keys()).union(set(solr_data.keys()))

    if len(all_keys) == 0:
        print("No data found. Aborting.")
        return

    for harvest_id in all_keys:
        rikolti_record = mapped_metadata.get(harvest_id)
        solr_record = solr_data.get(harvest_id)

        validator.validate(harvest_id, rikolti_record, solr_record)

    return validator


def create_collection_validation_csv(collection_id: int) -> None:
    result = validate_collection(collection_id)
    result.errors.output_csv_to_bucket(collection_id)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate mapped metadata against SOLR")
    parser.add_argument('collection_id', help='Collection ID')
    args = parser.parse_args(sys.argv[1:])
    collection_report = create_collection_validation_csv(args.collection_id)
    print(collection_report)
