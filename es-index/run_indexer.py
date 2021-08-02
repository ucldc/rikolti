import sys
import argparse
import boto3
import json
import os
import requests
from requests.auth import HTTPBasicAuth

ES_HOST = os.environ['ES_HOST']
ES_USER = os.environ['ES_USER']
ES_PASS = os.environ['ES_PASS']
BUCKET = os.environ['S3_BUCKET']

session = boto3.Session(profile_name='default')


def get_registry_data(col_id):
    ''' get registry data for this collection '''
    registry = {
        'collection_ids': [int(col_id)],
        'collection_urls': [(
            f"https://registry.cdlib.org/api/v1/collection/{col_id}/")],
        'repository_ids': [],
        'repository_urls': [],
        'repository_data': []
    }

    r = requests.get(registry['collection_urls'][0])
    col_details = r.json()

    registry['collection_data'] = [(f"{col_id}::{col_details.get('name')}")]
    for repo in col_details.get('repository'):
        registry['repository_ids'].append(repo.get('id'))
        registry['repository_urls'].append(
            f"https://registry.cdlib.org/api/v1/repository/{repo.get('id')}")

        data = f"{repo.get('id')}::{repo.get('name')}"
        if repo.get('campus'):
            campus_names = [c.get('name') for c in repo.get('campus')]
            campus_names = (', ').join(campus_names)
            data = f"{data}::{campus_names}"
        registry['repository_data'].append(data)

    return registry


def main(col_id):
    ''' get the objects to add to the index '''
    prefix = 'joined/collection_id={0}/'
    index = 'calisphere-items'
    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET)

    registry_data = get_registry_data(col_id)
    for obj in bucket.objects.filter(Prefix=prefix.format(col_id)):
        body = obj.get()['Body'].read()
        lines = body.splitlines()
        for line in lines:
            line = str(line, 'utf-8')
            document = json.loads(line)

            calisphere_id = document['calisphere-id'].split('--')
            item_id = calisphere_id[1]

            document.update(registry_data)
            document.update({'calisphere-id': item_id})

            # doc_id should be calisphere_id
            url = f'{ES_HOST}/{index}/_doc/{item_id}'
            headers = {"Content-Type": "application/json"}
            r = requests.put(
                url, auth=HTTPBasicAuth(ES_USER, ES_PASS),
                json=document, headers=headers)
            print(r.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run ES indexer')
    parser.add_argument('collection', help='collection id')
    '''
    parser.add_argument('bucket', help='S3 bucket')
    parser.add_argument('prefix', help='S3 prefix')
    parser.add_argument('20201005', help='ES index name')

    bucket = argv.bucket
    prefix = argv.prefix
    index = argv.index
    '''

    argv = parser.parse_args()
    collection_id = argv.collection

    sys.exit(main(collection_id))
