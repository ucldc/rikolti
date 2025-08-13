import json
import functools
import sys
from typing import Type
from collections import Counter

import requests
import urllib3

from . import settings, utilities
from .validator.validation_log import ValidationLogLevel
from .validator.validation_mode import ValidationMode
from .validator.validator import Validator
from rikolti.utils.versions import (
    get_versioned_page_as_json, get_version, get_versioned_pages)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def validate_collection(collection_id: int,
                        mapped_page_paths: list[str],
                        validator_class: Type[Validator] = None,
                        validator: Validator = None,
                        validation_mode = ValidationMode.STRICT,
                        log_level = ValidationLogLevel.WARNING,
                        verbose = False
                        ) -> Validator:
    """
    Validates all pages of a collection of mapped data.

    Parameters:
        collection_id: int
            The collection ID
        mapped_page_paths: list[str]
            A list of the relative paths to pages of vernacular metadata, ex:
            [
                3433/vernacular_metadata_v1/mapped_metadata_v1/data/1.jsonl,
                3433/vernacular_metadata_v1/mapped_metadata_v1/data/2.jsonl,
                3433/vernacular_metadata_v1/mapped_metadata_v1/data/3.jsonl
            ]
        validator_class: Type[Validator] (default: None)
            The validator class to use. Can be derived if not provided.
        validator: Validator (default: None)
            A validator instance to use. Can be derived if not provided.
        validation_mode: ValidationMode (default: ValidationMode.STRICT)
            The validation mode to use (unless overridden in individual validations)
        log_level: ValidationLogLevel (default: ValidationLogLevel.WARNING)
            The lowest log level that should be included in the output
        verbose: bool (default: False)
            Include verbose output in file?

    Returns: Validator
        The validator containing errors
    """
    if not validator_class and not validator:
        validator_class = get_validator_class(collection_id)

    if not validator:
        validator = validator_class(validation_mode=validation_mode,
                                    log_level = log_level,
                                    verbose = verbose)

    all_rikolti_ids = []
    new_rikolti_ids = []
    for page_path in mapped_page_paths:
        rikolti_ids, new_ids = validate_page(collection_id, page_path, validator)
        all_rikolti_ids += rikolti_ids
        new_rikolti_ids += new_ids

    id_counter = Counter(all_rikolti_ids)
    dupes = [(item, count) for item, count in id_counter.items() if count > 1]
    if dupes:
        dupes.sort(key=lambda x: x[1], reverse=True)
        num_dupes = sum([count for _, count in dupes]) - len(dupes)
        print(
            f"ERROR: {num_dupes} duplicate harvest_ids found in rikolti data:")
        print(dupes)

    # check all objects in rikolti against objects in solr
    # num_lost_solr_ids, num_solr_records = validate_collection_from_solr(
    #     collection_id, all_rikolti_ids, validator)
    # Disable solr validation for now
    num_lost_solr_ids = 0
    len(all_rikolti_ids)

    metadata_errors = (
        len(validator.log.log) - len(new_rikolti_ids) - num_lost_solr_ids)

    print(
        f"{collection_id:<6}, "
        f"{metadata_errors} validation errors, "
        f"{len(new_rikolti_ids)} new records, "
        #f"{num_lost_solr_ids} lost records, "
        "Unknown number of lost records while validation is disabled, "
        f"{len(all_rikolti_ids)} rikolti records, "
        #f"{num_solr_records} solr records"
        "Unknown number of solr records while validation is disabled"
    )

    return validator

def validate_collection_from_solr(
        collection_id: int, all_rikolti_ids: list[str], validator: Validator):
    # check all objects in solr against objects in rikolti to track lost objects
    solr_ids = get_all_solr_records_in_collection(collection_id)
    lost_solr_ids = list(set(solr_ids).difference(set(all_rikolti_ids)))

    if lost_solr_ids:
        solr_data = get_solr_data(collection_id, lost_solr_ids)
        for solr_record in solr_data:
            validator.log.add(
                key=solr_record['harvest_id_s'],
                field="missing record",
                description="No mapped rikolti data found",
                expected=solr_record,
                actual=None,
                level=ValidationLogLevel.ERROR,
                **validator.log_entry_urls(solr_record['harvest_id_s'])
            )
    return len(lost_solr_ids), len(solr_ids)

def get_all_solr_records_in_collection(collection_id: int):
    collection_url = ("collection_url:\"https://registry.cdlib.org/api/v1/"
                      f"collection/{collection_id}/\"")

    query = {
        'fq': collection_url,
        'start': 0,
        'rows': 5000,
        'fl': "harvest_id_s",
    }

    response = make_solr_request(**query)
    solr_ids = [
        doc['harvest_id_s'] for doc in
        response.get("response", {}).get("docs", [])
    ]
    num_fetched = len(solr_ids)
    num_found = response.get("response", {}).get("numFound", 0)

    while num_fetched < num_found:
        query['start'] = num_fetched
        response = make_solr_request(**query)
        solr_ids = solr_ids + [
            doc['harvest_id_s'] for doc in
            response.get("response", {}).get("docs", [])
        ]
        num_fetched = len(solr_ids)

    return solr_ids

def validate_page(collection_id: int, page_path: str,
                  validator: Validator) -> tuple[list[str], list]:
    """
    Validates a provided page of a provided collection of mapped data.

    Parameters:
        collection_id: int
            The collection ID
        page_path: str
            The relative path to a specific page of mapped metadata, ex:
                3433/vernacular_metadata_v1/mapped_metadata_v1/data/1.jsonl
        validator: Validator
            The validator instance to use

    Returns Validator
        The validator containing validation errors
    """
    context = {
        "collection_id": collection_id,
        "page_path": page_path
    }
    collection = get_versioned_page_as_json(page_path)

    if len(collection) == 0:
        print(
            f"WARNING: No mapped metadata found for {collection_id} "
            f"page {page_path}."
        )
        return [],[]

    mapped_metadata = validator.generate_keys(
                        collection,
                        type="Rikolti",
                        context=context
                      )
    comparison_data = validator.generate_keys(
                        get_comparison_data(collection_id,
                                            list(mapped_metadata.keys())),
                        type="Solr",
                        context=context
                      )

    all_keys = set(mapped_metadata.keys()).union(set(comparison_data.keys()))

    if len(all_keys) == 0:
        raise ValueError("No rikolti or solr data found. Aborting.")

    new_records = []
    for harvest_id in all_keys:
        rikolti_record = mapped_metadata.get(harvest_id)
        solr_record = comparison_data.get(harvest_id)

        if not solr_record:
            validator.log.add(
                key=harvest_id,
                field="new record",
                description="No Solr data found",
                expected=None,
                actual=rikolti_record,
            )
            new_records.append(harvest_id)
        elif not rikolti_record:
            raise ValueError(f"No rikolti data found for {harvest_id}")
        else:
            validator.validate(harvest_id, rikolti_record, solr_record)

    return list(mapped_metadata.keys()), new_records


def create_collection_validation_csv(
        collection_id: int, mapped_page_paths: list[str], **options) -> tuple[int, str]:
    result = validate_collection(collection_id, mapped_page_paths, **options)

    mapped_version = get_version(collection_id, mapped_page_paths[0])
    filename = result.log.output_csv_to_bucket(collection_id, mapped_version)
    return len(result.log.log), filename

## Private-ish


def get_comparison_data(collection_id: int, harvest_ids: list[str]) -> list[dict]:
    solr_data = get_solr_data(collection_id, harvest_ids)
    couch_data = get_couch_db_data(collection_id, harvest_ids)

    return [
      {**solr_dict, **(couch_data[solr_dict["harvest_id_s"]] or {})}
      for solr_dict in solr_data
    ]


def get_solr_data(collection_id: int, harvest_ids: list[str]) -> list[dict]:
    # solr often times out, limit this to sets of 50 harvest_ids.
    solr_batch_size = 50
    solr_data = []
    for i in range(0, len(harvest_ids), solr_batch_size):
        solr_batch = harvest_ids[i:i+solr_batch_size]

        collection_url = ("collection_url:\"https://registry.cdlib.org/api/v1/"
                        f"collection/{collection_id}/\"")
        harvest_id_q = " OR ".join([f"harvest_id_s:\"{f}\"" for f in solr_batch])
        query = {
            'fq': f"{collection_url} AND ({harvest_id_q})",
            'rows': len(solr_batch),
            'start': 0
        }

        response = make_solr_request(**query)
        solr_data = solr_data + response.get("response", {"docs": None}).get("docs", [])
    return solr_data


def make_solr_request(**params):
    """Makes the request against Solr based on provided params"""
    solr_url = f'{settings.SOLR_URL}/query/'
    solr_auth = {'X-Authentication-Token': settings.SOLR_API_KEY} 
    query = {}
    for key, value in list(params.items()):
        key = key.replace('_', '.')
        query.update({key: value})
    try:
        res = requests.post(solr_url, headers=solr_auth, data=query, verify=False)
        res.raise_for_status()
    except Exception as e:
        print(f"Error making post request to {solr_url}", file=sys.stderr)
        print(f"Errored solr query: {query}", file=sys.stderr)
        raise(e)
    return json.loads(res.content.decode('utf-8'))


@functools.lru_cache(maxsize=3)
def couch_db_request(collection_id: int, field_name: str) -> list[dict[str, str]]:
    """
    Requests isShownAt and iShownBy data from Couch DB.
    This method gets all the data for a collection, but is invoked
    for each page of a collection, so we cache the result in an
    LRU cache to avoid hammering Couch DB.

    I'm taking a conservative approach to the cache size here, since
    the size of the response from Couch DB will be highly variable.
    A single collection can be successfully validated with two entries
    in the cache: one for isShownAt and one for isShownBy, so I set
    the maxsize to 3.

    Returns: list[dict]
    """
    print("Couchdb is no longer running. "
           "Continuing without isShownAt and isShownBy values, "
           "which may result in increased/inaccurate validation errors.")
    return []

def get_couch_db_data(collection_id: int,
                      harvest_ids: list[str]) -> dict[str, dict[str, str]]:
    """
    Parses down the data returned from the Couch DB request to only
    the data we need for the harvest_ids we're validating.

    Data is returned as a list of single-entry dicts, so we'll
    parse them out into a dict[str, dict[str, str]] where the keys are
    harvest_ids and the values are dicts: {field_name: field_value}
    """
    # This method gets ALL data for the collection, then pares it down

    def snakeify(s):
        res = ""
        for i in s:
            if i.isupper():
                res += "_" + i.lower()
            else:
                res += i
        return res

    ret = {}

    for field_name in ["isShownAt", "isShownBy"]:  
        couch_data = couch_db_request(collection_id, field_name)
        data = {}
        for d in couch_data:
            key = list(d.keys())[0]
            data[key] = {snakeify(field_name): d[key]}

        ret = {
            key: {**(ret.get(key) or {}), **(data.get(key) or {})}
            for key in harvest_ids
        }

    return ret


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
    vernacular = utilities.import_vernacular_reader(mapper)

    return vernacular.validator if hasattr(vernacular, "validator") else Validator


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate mapped metadata against SOLR")
    
    parser.add_argument('collection_id', help='Collection ID')
    parser.add_argument('mapped_data_version', help="Mapped data version, ex: 3433/vernacular_data_1/mapped_data_1")
    parser.add_argument("--log-level", dest="log_level",
                        help="Log level - can be ERROR, WARNING, INFO, or DEBUG")
    parser.add_argument('-v', '--verbose', action="store_true", help="Verbose mode")
    parser.add_argument('-f', '--filename', dest="filename", help="Output filename")

    args = parser.parse_args(sys.argv[1:])

    opt_args = ["log_level", "verbose", "filename"]
    kwargs = {attrname: getattr(args, attrname)
              for attrname in opt_args
              if getattr(args, attrname)}

    if kwargs.get("log_level"):
      kwargs["log_level"] = getattr(ValidationLogLevel, args.log_level.upper())

    print(f"Generating validations for collection {args.collection_id} with options:")
    print(kwargs)

    mapped_page_paths = get_versioned_pages(args.mapped_data_version)

    num_rows, file_location = create_collection_validation_csv(
        args.collection_id, mapped_page_paths, **kwargs)
    print(f"Output {num_rows} rows to {file_location}")
