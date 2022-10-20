import json
import os
import sys
from urllib.parse import urlparse, parse_qs

from nuxeo_mapper import NuxeoVernacular
from mapper import UCLDCWriter, Record
from oac_mapper import OAC_Vernacular

DEBUG = os.environ.get('DEBUG', False)


def get_source_vernacular(payload):
    source_type = payload.get('mapper_type')
    if source_type == 'NuxeoMapper':
        return NuxeoVernacular(payload)
    if source_type == 'oac_dc':
        return OAC_Vernacular(payload)


def parse_enrichment_url(enrichment_url):
    enrichment_func = (urlparse(enrichment_url)
                       .path
                       .strip('/')
                       .replace('-', '_'))
    kwargs = parse_qs(urlparse(enrichment_url).query)
    if enrichment_func not in dir(Record):
        raise Exception(f"ERROR: {enrichment_func} not found in {Record}")
    return enrichment_func, kwargs


# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": "r-0"}
# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": 2}
def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)

    vernacular = get_source_vernacular(payload)
    api_resp = vernacular.get_api_response()
    source_metadata_records = vernacular.parse(api_resp)
    collection = payload.get('collection', {})

    for enrichment_url in collection.get('rikolti__pre_mapping'):
        enrichment_func, kwargs = parse_enrichment_url(enrichment_url)
        print(f"running enrichment: {enrichment_func} with {kwargs}")
        source_metadata_records = [
            record.enrich(enrichment_func, **kwargs)
            for record in source_metadata_records
        ]

    mapped_records = [record.to_UCLDC() for record in source_metadata_records]
    writer = UCLDCWriter(payload)
    if DEBUG:
        writer.write_local_mapped_metadata(
            [record.to_dict() for record in mapped_records])

    for enrichment_url in collection.get('rikolti__enrichments'):
        enrichment_func, kwargs = parse_enrichment_url(enrichment_url)
        if enrichment_func in ['required_values_from_collection_registry',
                               'set_ucldc_dataprovider']:
            kwargs.update({'collection': collection})
        print(f"running enrichment: {enrichment_func} with {kwargs}")
        mapped_records = [
            record.enrich(enrichment_func, **kwargs)
            for record in mapped_records
        ]

    # some enrichments had previously happened at ingest into Solr
    # TODO: these are just two, investigate further
    # mapped_records = [record.add_sort_title() for record in mapped_records]
    # mapped_records = [record.map_registry_data(collection)
    #                   for record in mapped_records]

    mapped_metadata = [record.to_dict() for record in mapped_records]
    if DEBUG:
        writer.write_local_mapped_metadata(mapped_metadata)
    else:
        writer.write_s3_mapped_metadata([
            record.to_dict() for record in mapped_records])

    return {
        'statusCode': 200,
        'body': json.dumps(payload)
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Map metadata from the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    lambda_handler(args.payload, {})
