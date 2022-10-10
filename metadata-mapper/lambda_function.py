import json
import os
import boto3
import sys
import subprocess

from nuxeo_mapper import NuxeoParser
from mapper import UCLDCWriter
from oac_mapper import OAC_Parser

DEBUG = os.environ.get('DEBUG', False)

def get_source_parser(source_type):
    if source_type == 'nuxeo':
        return NuxeoParser
    if source_type == 'oac_dc':
        return OAC_Parser

# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": "r-0"}
# {"collection_id": 26098, "source_type": "nuxeo", "page_filename": 2}
def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)
    
    parser_cls = get_source_parser(payload.get('source_type'))
    print(parser_cls)
    source = parser_cls(payload)
    if DEBUG:
        api_resp = source.get_local_api_response()
    else:
        api_resp = source.get_s3_api_response()

    source_metadata_records = source.parse(api_resp)
    mapped_metadata = [record.to_UCLDC() for record in source_metadata_records]

    writer = UCLDCWriter(payload)
    if DEBUG:
        writer.write_local_mapped_metadata(mapped_metadata)
    else:
        writer.write_s3_mapped_metadata(mapped_metadata)

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
