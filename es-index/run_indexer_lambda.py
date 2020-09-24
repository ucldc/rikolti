import boto3
import re
import requests
from requests_aws4auth import AWS4Auth
from nuxeo_indexer import NuxeoESIndexer

''' 
    lambda function for S3 to use as event handler when new content arrives in relevant bucket
    https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-aws-integrations.html#es-aws-integrations-s3-lambda-es
'''
region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-pachamama-dev-public-w7mykn7bucrpqksnm7tu3vg3za.us-east-1.es.amazonaws.com' # the Amazon ES domain, including https://
index = ''
type = ''
url = host + '/' + index + '/' + type

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

# Lambda execution starts here
def handler(event, context):
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read() # these should each be one jsonl file
        
        # add new document to index
        indexer = NuxeoESIndexer(body)

