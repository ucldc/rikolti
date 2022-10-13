import json
import os
import sys
from urllib.parse import urlparse, parse_qs

from nuxeo_mapper import NuxeoVernacular
from mapper import UCLDCWriter
from oac_mapper import OAC_Vernacular

DEBUG = os.environ.get('DEBUG', False)


def get_source_vernacular(source_type):
    if source_type == 'NuxeoMapper':
        return NuxeoVernacular
    if source_type == 'oac_dc':
        return OAC_Vernacular


# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": "r-0"}
# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": 2}
def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)

    vernacular_cls = get_source_vernacular(payload.get('mapper_type'))
    vernacular = vernacular_cls(payload)
    if DEBUG:
        api_resp = vernacular.get_local_api_response()
    else:
        api_resp = vernacular.get_s3_api_response()

    source_metadata_records = vernacular.parse(api_resp)

    enrichment_chain = payload.get('enrichments', {})
    for enrichment in enrichment_chain.get('pre_mapping'):
        enrichment_func = (urlparse(enrichment)
                           .path
                           .strip('/')
                           .replace('-', '_'))
        kwargs = parse_qs(urlparse(enrichment).query)
        if enrichment_func not in dir(type(source_metadata_records[0])):
            print(
                f"ERROR: {enrichment_func} not found in "
                f"{type(source_metadata_records[0])}",
                file=sys.stderr
            )
            continue
        print(
            f"running enrichment: {enrichment_func} with "
            f"{kwargs}"
        )
        source_metadata_records = [
            record.enrich(enrichment_func, **kwargs)
            for record in source_metadata_records
        ]

    mapped_records = [record.to_UCLDC() for record in source_metadata_records]

    writer = UCLDCWriter(payload)
    if DEBUG:
        writer.write_local_mapped_metadata(
            [record.to_dict() for record in mapped_records])

    for e in enrichment_chain.get('enrichments'):
        enrichment_function = urlparse(e).path.strip('/').replace('-', '_')
        function_parameters = parse_qs(urlparse(e).query)
        if enrichment_function not in dir(type(source_metadata_records[0])):
            print(
                f"ERROR: {enrichment_function} not found in "
                f"{type(source_metadata_records[0])}",
                file=sys.stderr
            )
            continue
        print(
            f"running enrichment: {enrichment_function} with "
            f"{function_parameters}"
        )
        mapped_records = [
            record.enrich(enrichment_function, **function_parameters)
            for record in mapped_records
        ]

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
