import sys
import boto3
import json
from nuxeo_indexer import NuxeoESIndexer

bucket = 'ucldc-ingest'
prefix = 'glue-test-data-target/2020_03_19_0022/20200521_104757/'
index = 'testing'

session = boto3.Session(profile_name='pachamamaES')

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
            indexer.index_document(indexer.document, es_id)      

def increment_id(current_id):
    return current_id + 1

if __name__ == "__main__":
    sys.exit(main(bucket, prefix, index))
