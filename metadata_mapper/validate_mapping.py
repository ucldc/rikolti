import json
import functools
import sys
from typing import Type

import requests
import urllib3

from . import settings, utilities
from .validator.validation_log import ValidationLogLevel
from .validator.validation_mode import ValidationMode
from .validator.validator import Validator
from rikolti.utils.rikolti_storage import get_page_content

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

    for page_path in mapped_page_paths:
        validate_page(collection_id, page_path, validator)

    return validator


def validate_page(collection_id: int, page_path: str,
                  validator: Validator) -> Validator:
    """
    Validates a provided page of a provided collection of mapped data.

    Parameters:
        collection_id: int
            The collection ID
        page_path: str
            The absolute path to a page within the collection
        validator: Validator
            The validator instance to use

    Returns Validator
        The validator containing validation errors
    """
    context = {
        "collection_id": collection_id,
        "page_path": page_path
    }
    mapped_metadata = validator.generate_keys(
                        get_mapped_data(page_path),
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


def create_collection_validation_csv(
        collection_id: int, mapped_page_paths: list[str], **options) -> tuple[int, str]:
    result = validate_collection(collection_id, mapped_page_paths, **options)

    filename = result.log.output_csv_to_bucket(collection_id, mapped_page_paths[0])
    return len(result.log.log), filename

## Private-ish


def get_mapped_data(page_path: str) -> list[dict]:
    return json.loads(get_page_content(page_path))


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
    url = f"{settings.COUCH_URL}/" \
        "couchdb/ucldc/_design/all_provider_docs/" \
        "_list/has_field_value/by_provider_name_wdoc" \
        f"?key=\"{collection_id}\"&field={field_name}&limit=100000"
    response = requests.get(url, verify=False)
    # TODO: add a timeout to this request & if it times out, signal to the user
    # that the request timed out and validation is continuing without data from
    # Couch DB - a message to std.out should suffice
    return json.loads(response.content)


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

    num_rows, file_location = create_collection_validation_csv(
        args.collection_id, mapped_page_paths, **kwargs)
    print(f"Output {num_rows} rows to {file_location}")
