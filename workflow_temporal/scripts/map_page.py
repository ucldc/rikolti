import importlib
import json

from typing import Union
from urllib.parse import urlparse, parse_qs

import settings
import logging

import sys, os
root_folder = os.path.abspath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root_folder)
from metadata_mapper.mappers.mapper import UCLDCWriter, Record, Vernacular

def import_vernacular_reader(mapper_type):
    """
    accept underscored_module_name_prefixes
    accept CamelCase class name prefixes split on underscores
    for example:
    mapper_type       | mapper module       | class name
    ------------------|---------------------|------------------
    nuxeo.nuxeo       | nuxeo_mapper        | NuxeoVernacular
    oai.content_dm    | content_dm_mapper   | ContentDmVernacular
    """
    *mapper_parent_modules, snake_cased_mapper_name = mapper_type.split(".")

    mapper_module = importlib.import_module(
        f"metadata_mapper.mappers.{'.'.join(mapper_parent_modules)}.{snake_cased_mapper_name}_mapper",
        package="metadata_mapper"
    )

    mapper_type_words = snake_cased_mapper_name.split('_')
    class_type = ''.join([word.capitalize() for word in mapper_type_words])
    vernacular_class = getattr(
        mapper_module, f"{class_type}Vernacular")

    if not issubclass(vernacular_class, Vernacular):
        print(f"{mapper_type} not a subclass of Vernacular")
        exit()
    return vernacular_class

def parse_enrichment_url(enrichment_url):
    enrichment_func = (urlparse(enrichment_url)
                       .path
                       .strip('/')
                       .replace('-', '_'))
    kwargs = parse_qs(urlparse(enrichment_url).query)
    if enrichment_func not in dir(Record):
        if settings.SKIP_UNDEFINED_ENRICHMENTS:
            return None, None
        else:
            raise Exception(f"ERROR: {enrichment_func} not found in {Record}")
    return enrichment_func, kwargs

def run_enrichments(records, payload, enrichment_set):
    collection = payload.get('collection', {})
    for enrichment_url in collection.get(enrichment_set, []):
        enrichment_func, kwargs = parse_enrichment_url(enrichment_url)
        if not enrichment_func and settings.SKIP_UNDEFINED_ENRICHMENTS:
            continue
        if enrichment_func in ['required_values_from_collection_registry',
                               'set_ucldc_dataprovider']:
            kwargs.update({'collection': collection})
        logging.debug(
            f"[{collection['id']}]: running enrichment: {enrichment_func} "
            f"for page {payload['page_filename']} with kwargs: {kwargs}")
        records = [
            record.enrich(enrichment_func, **kwargs)
            for record in records
        ]
    return records

def map_page(payload: dict):
    payload = json.loads(payload)
    mapper_type = payload.get('collection').get('rikolti_mapper_type')
    vernacular_reader = import_vernacular_reader(mapper_type)
    source_vernacular = vernacular_reader(payload)
    api_resp = source_vernacular.get_api_response()
    source_metadata_records = source_vernacular.parse(api_resp)

    source_metadata_records = run_enrichments(
        source_metadata_records, payload, 'rikolti__pre_mapping')

    for record in source_metadata_records:
        record.to_UCLDC()
    mapped_records = source_metadata_records

    writer = UCLDCWriter(payload)
    if settings.DATA_DEST == 'local':
        writer.write_local_mapped_metadata(
            [record.to_dict() for record in mapped_records])

    #mapped_records = run_enrichments(
    #    mapped_records, payload, 'rikolti__enrichments')
    # Exception: ERROR: enrich_earliest_date not found in <class 'metadata_mapper.mappers.mapper.Record'>

    #mapped_records = [record.solr_updater() for record in mapped_records]
    '''
      File "/usr/local/dev/rikolti/metadata_mapper/mappers/mapper.py", line 1466, in map_couch_to_solr_doc
        collections = record['collection']
                      ~~~~~~^^^^^^^^^^^^^^
    KeyError: 'collection'
    '''

    mapped_records = [record.remove_none_values() for record in mapped_records]
    mapped_metadata = [record.to_dict() for record in mapped_records]
    writer.write_local_mapped_metadata(mapped_metadata)
