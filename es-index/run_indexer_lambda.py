import boto3
import json
from nuxeo_indexer import NuxeoESIndexer

''' 
    lambda function for S3 to use as event handler when new content arrives in relevant bucket
    https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-aws-integrations.html#es-aws-integrations-s3-lambda-es
'''
bucket = 'rikolti'
prefix = 'joined/collection_id='
index = 'testing'

s3 = boto3.client('s3')

# Lambda execution starts here
def handler(event, context):

    es_id = 0 # dumb for now
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read() # these should each be one jsonl file
        lines = body.splitlines()
        for line in lines:
            line = str(line, 'utf-8') # Convert from bytestring to string to avoid "Object of type 'bytes' is not JSON serializable" error. Might be better to do this elsewhere?
            indexer = NuxeoESIndexer(line)
            es_id = increment_id(es_id)
            indexer.index_document(indexer.document, es_id, index)
       
def increment_id(current_id):
    return current_id + 1 
