import json
import os
import boto3
import sys
import subprocess

from oac_mapper import OAC_DCMapper

DEBUG = os.environ.get('DEBUG', False)

def lambda_handler(payload, context):
    payload = json.loads(payload)

    collection_id = payload.get('collection_id')
    if not collection_id:
        print('no collection id!')

    s3 = boto3.resource('s3')
    rikolti_bucket = s3.Bucket('rikolti')
    prefix = f"vernacular_metadata/{collection_id}/"
    keys = rikolti_bucket.objects.filter(Prefix=prefix)

    for obj_summary in keys:
        s3_obj = obj_summary.get()
        for line in s3_obj['Body'].iter_lines():
            vernacular_metadata = json.loads(line.decode('utf-8'))
            print(vernacular_metadata)
            mapped_metadata = OAC_DCMapper(vernacular_metadata).map()
            print(mapped_metadata)


    # fetcher.fetch_page()
    # next_page = fetcher.json()
    # if next_page:
    #     if DEBUG:
    #         subprocess.run([
    #             'python',
    #             'lambda_function.py',
    #             next_page.encode('utf-8')
    #         ])
    #     else:
    #         lambda_client = boto3.client('lambda', region_name="us-west-2",)
    #         lambda_client.invoke(
    #             FunctionName="fetch-metadata",
    #             InvocationType="Event",  # invoke asynchronously
    #             Payload=next_page.encode('utf-8')
    #         )

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
