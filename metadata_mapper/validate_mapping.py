import json
import requests
import sys
import urllib3

from typing import Type

import settings
import utilities

from validator.validator import Validator
from validator.validation_mode import ValidationMode
from validator.validation_log import ValidationLogLevel

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def validate_collection(collection_id: int,
                        validator_class: Type[Validator] = None,
                        validator: Validator = None,
                        default_validation_mode = ValidationMode.STRICT,
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

    Returns: Validator
        The validator containing errors
    """
    if not validator_class and not validator:
        validator_class = get_validator_class(collection_id)

    if not validator:
        validator = validator_class(default_validation_mode=default_validation_mode,
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

    if len(mapped_metadata) == 0 or len(comparison_data) == 0:
        print("No data found in "
              f"{'mapped Rikolti data' if len(mapped_metadata) else 'Solr'}."
              " Aborting."
              )
        return

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
    ret = {}

    for field_name in ["isShownAt", "isShownBy"]:  
      url = "https://harvest-prd.cdlib.org/" \
            "couchdb/ucldc/_design/all_provider_docs/" \
            "_list/has_field_value/by_provider_name_wdoc" \
            f"?key=\"{collection_id}\"&field={field_name}&limit=1000"
      
      #### TODO: REMOVE verify=False once cert issues are resolved!
      response = requests.get(url, verify=False)
      data = {}
      for d in json.loads(response.content):
          key = list(d.keys())[0]
          data[key] = {field_name: d[key]}

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

    return vernacular.record_cls.validator


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
    kwargs = {attrname: getattr(args, attrname) for attrname in opt_args if getattr(args, attrname)}

    if kwargs.get("log_level"):
      kwargs["log_level"] = getattr(ValidationLogLevel, args.log_level.upper())

    print(f"Generating validation file for collection {args.collection_id} with options:")
    print(kwargs)

    collection_report = create_collection_validation_csv(args.collection_id, **kwargs)
    print(collection_report)
