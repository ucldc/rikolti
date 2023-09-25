import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Type

import requests
import urllib3

from . import settings, utilities
from .validator.validation_log import ValidationLogLevel
from .validator.validation_mode import ValidationMode
from .validator.validator import Validator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def validate_collection(collection_id: int,
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
    comparison_data = validator.generate_keys(
                        get_comparison_data(collection_id,
                                            list(mapped_metadata.keys())),
                        type="Solr",
                        context=context
                      )

    # if len(mapped_metadata) == 0 or len(comparison_data) == 0:
    #     print("No data found in "
    #           f"{'mapped Rikolti data' if len(mapped_metadata) == 0 else 'Solr'}."
    #           " Aborting."
    #           )
    #     return

    all_keys = set(mapped_metadata.keys()).union(set(comparison_data.keys()))

    if len(all_keys) == 0:
        print("No data found. Aborting.")
        return

    for harvest_id in all_keys:
        rikolti_record = mapped_metadata.get(harvest_id)
        solr_record = comparison_data.get(harvest_id)

        validator.validate(harvest_id, rikolti_record, solr_record)

    return validator


def create_collection_validation_csv(collection_id: int, **options) -> None:
    result = validate_collection(collection_id, **options)
    filename = result.log.output_csv_to_bucket(collection_id)
    return f"Output {len(result.log.log)} rows to {filename}"

def compare_collection_validation(collection_id: int, filename: str = None,
                                  **options) -> None:
    old_filename = utilities.get_files("validation", collection_id)[-1]
    old_filepath = utilities.get_local_bucket_filepath("validation",
                                                       collection_id,
                                                       old_filename)
    if not Path(old_filepath).is_file():
        print("Previous validation data not found. Exiting.")
        return

    print("Running validator")
    result = validate_collection(collection_id, **options)
    new_filename = result.log.output_csv_to_bucket(collection_id)
    new_filepath = utilities.get_local_bucket_filepath("validation",
                                                       collection_id,
                                                       new_filename)

    print("Running comparison")
    with open(new_filepath, "r") as new_file:
        new = csv.DictReader(new_file, delimiter=",")
        with open(old_filepath, "r") as old_file:
            old = csv.DictReader(old_file, delimiter=",")

            for index, new_row in enumerate(new):
                if new_row["Level"] in ["INFO", "DEBUG"]:
                    continue
                
                # Find old row(s) that match ID and field
                old_rows = [
                    [index, old_row] for old_row in old
                    if old_row["Harvest ID"] == new_row["Harvest ID"]
                        and old_row["Field"] == new_row["Field"]
                        and bool(old_row["Field"])
                        and old_row["Level"] not in ["INFO", "DEBUG"]
                        and old_row["Description"] != new_row["Description"]
                ]

                if not old_rows:
                    return "No changes"

                ret = [["Harvest ID", "Field", "Reason", "New Value", "Old Value"]]
                for (index, old_row) in old_rows:
                    ret.append([
                        old_row["Harvest ID"],
                        old_row["Field"],
                        "Description changed",
                        new_row["Description"],
                        old_row["Description"]
                    ])

                content_string = "\n".join([
                    ",".join(row)
                    for row in ret
                ])

                if not filename:
                    filename = f"{datetime.now().strftime('%m-%d-%YT%H:%M:%S')}.csv"

                utilities.write_to_bucket("validation_comparisons",
                                          collection_id,
                                          filename,
                                          content_string)
            
                return filename


## Private-ish


def get_mapped_data(collection_id: int, page_id: int) -> dict:
    return utilities.read_mapped_metadata(collection_id, page_id)


def get_comparison_data(collection_id: int, harvest_ids: list[str]) -> list[dict]:
    solr_data = get_solr_data(collection_id, harvest_ids)
    couch_data = get_couch_db_data(collection_id, harvest_ids)

    return [
      {**solr_dict, **(couch_data[solr_dict["harvest_id_s"]] or {})}
      for solr_dict in solr_data
    ]


def get_solr_data(collection_id: int, harvest_ids: list[str]) -> list[dict]:
    collection_url = ("collection_url:\"https://registry.cdlib.org/api/v1/"
                      f"collection/{collection_id}/\"")
    harvest_id_q = " OR ".join([f"harvest_id_s:\"{f}\"" for f in harvest_ids])
    query = {
        'fq': f"{collection_url} AND ({harvest_id_q})",
        'rows': len(harvest_ids),
        'start': 0
    }

    response = make_solr_request(**query)
    return response.get("response", {"docs": None}).get("docs", [])


def make_solr_request(**params):
    """Makes the request against Solr based on provided params"""
    solr_url = f'{settings.SOLR_URL}/query/'
    solr_auth = {'X-Authentication-Token': settings.SOLR_API_KEY} 
    query = {}
    for key, value in list(params.items()):
        key = key.replace('_', '.')
        query.update({key: value})
    res = requests.post(solr_url, headers=solr_auth, data=query, verify=False)
    res.raise_for_status()
    return json.loads(res.content.decode('utf-8'))


def get_couch_db_data(collection_id: int,
                      harvest_ids: list[str]) -> dict[str, dict[str, str]]:
    """
    Requests isShownAt and isShownBy data from Couch DB.

    Data is returned as a list of single-entry dicts, so we'll
    parse them out into a dict[str, dict[str, str]] where the keys are
    harvest_ids and the 
    """
    # This method get ALL data for the collection, then pares it down
    # TODO: See if there's a way to improve the query to only get relevant data
    # TODO: See if there's a way to get both fields in a single query

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
        url = "https://harvest-prd.cdlib.org/" \
            "couchdb/ucldc/_design/all_provider_docs/" \
            "_list/has_field_value/by_provider_name_wdoc" \
            f"?key=\"{collection_id}\"&field={field_name}&limit=1000"
      
        response = requests.get(url, verify=False)
        data = {}
        for d in json.loads(response.content):
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

    collection_report = create_collection_validation_csv(args.collection_id, **kwargs)
    print(collection_report)
