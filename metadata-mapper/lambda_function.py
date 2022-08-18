import json
import os
import boto3
import sys
import subprocess

from mapper import Mapper
from oac_mapper import OAC_DCMapper
from nuxeo_mapper import NuxeoMapper

DEBUG = os.environ.get('DEBUG', False)

def get_mapper(payload):
    mapper_type = payload.get('mapper_type')
    if mapper_type == 'NuxeoMapper':
        return NuxeoMapper(payload)


# {"collection_id": 26098, "mapper_type": "NuxeoMapper", "page": "r-0"}
# {"collection_id": 26098, "mapper_type": "NuxeoMapper", "page": 2}
def lambda_handler(payload, context):
    if DEBUG:
        payload = json.loads(payload)
    mapper = get_mapper(payload)
    mapper.map_page()

    next_page = mapper.increment()
    if next_page:
        lambda_handler(json.dumps(next_page), {})

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
