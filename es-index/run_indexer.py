import sys
import argparse
import boto3
import json
from nuxeo_indexer import NuxeoESIndexer

bucket = 'ucldc-ingest'
prefix = 'glue-test-data-target/joined/27414/'
index = '20201005'

session = boto3.Session(profile_name='default')

def main(bucket, prefix, index):

    ''' get the objects to add to the index '''
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket)

    es_id = 0 # just dumbly incrementing the id for now
    for obj in bucket.objects.filter(Prefix=prefix):
        body = obj.get()['Body'].read()
        lines = body.splitlines()
        for line in lines:
            line = str(line, 'utf-8') # Convert from bytestring to string to avoid "Object of type 'bytes' is not JSON serializable" error. Might be better to do this elsewhere?
            indexer = NuxeoESIndexer(line)
            es_id = increment_id(es_id)
            indexer.index_document(indexer.document, es_id, index)      

def increment_id(current_id):
    return current_id + 1

if __name__ == "__main__":

    '''
    parser = argparse.ArgumentParser(
        description='Run ES indexer')
    parser.add_argument('bucket', help='S3 bucket')
    parser.add_argument('prefix', help='S3 prefix')
    parser.add_argument('20201005', help='ES index name')

    argv = parser.parse_args()

    bucket = argv.bucket
    prefix = argv.prefix
    index = argv.index
    '''

    sys.exit(main(bucket, prefix, index))
