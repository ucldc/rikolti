import sys
import argparse
import boto3
import json
import os
import requests
from requests.auth import HTTPBasicAuth
import re

ES_HOST = os.environ['ES_HOST']
ES_USER = os.environ['ES_USER']
ES_PASS = os.environ['ES_PASS']
BUCKET = os.environ['S3_BUCKET']

session = boto3.Session(profile_name='default')

# from harvester.solr_updater
RE_ALPHANUMSPACE = re.compile(r'[^0-9A-Za-z\s]*')  # \W include "_" as does A-z


# from harvester.solr_updater
def normalize_sort_field(sort_field,
                         default_missing='~title unknown',
                         missing_equivalents=['title unknown']):
    sort_field = sort_field.lower()
    # remove punctuation
    sort_field = RE_ALPHANUMSPACE.sub('', sort_field)
    words = sort_field.split()
    if words:
        if words[0] in ('the', 'a', 'an'):
            sort_field = ' '.join(words[1:])
    if not sort_field or sort_field in missing_equivalents:
        sort_field = default_missing
    return sort_field


# updated from harvester.solr_updater
def add_sort_title(s3_doc):
    '''Add a sort title to the solr doc'''
    if isinstance(s3_doc['title'], str):
        sort_title = s3_doc['title']
    else:
        sort_title = s3_doc['title'][0]
    # if 'sort-title' in couch_doc['originalRecord']:  # OAC mostly
    #     sort_obj = couch_doc['originalRecord']['sort-title']
    #     if isinstance(sort_obj, list):
    #         sort_obj = sort_obj[0]
    #         if isinstance(sort_obj, dict):
    #             sort_title = sort_obj.get(
    #                 'text', couch_doc['sourceResource']['title'][0])
    #         else:
    #             sort_title = sort_obj
    #     else:  # assume flat string
    #         sort_title = sort_obj
    sort_title = normalize_sort_field(sort_title)
    return sort_title


def get_registry_data(col_id):
    ''' get registry data for this collection '''
    registry = {
        'collection_ids': [int(col_id)],
        'collection_urls': [(
            f"https://registry.cdlib.org/api/v1/collection/{col_id}/")],
        'sort_collection_data': [],
        'repository_ids': [],
        'repository_urls': [],
        'repository_data': [],
        'campus_ids': []
    }

    r = requests.get(registry['collection_urls'][0])
    col_details = r.json()

    sort_name = normalize_sort_field(
        col_details.get('name'),
        default_missing='~collection unknown',
        missing_equivalents=[])
    registry['sort_collection_data'] = '::'.join((
        sort_name, col_details.get('name'), col_id))

    registry['collection_data'] = [(f"{col_id}::{col_details.get('name')}")]
    for repo in col_details.get('repository'):
        registry['repository_ids'].append(repo.get('id'))
        registry['repository_urls'].append(
            f"https://registry.cdlib.org/api/v1/repository/{repo.get('id')}")

        data = f"{repo.get('id')}::{repo.get('name')}"
        if repo.get('campus'):
            campus_names = [c.get('name') for c in repo.get('campus')]
            campus_names = (', ').join(campus_names)
            campus_ids = [c.get('id') for c in repo.get('campus')]
            data = f"{data}::{campus_names}"
        registry['repository_data'].append(data)
        registry['campus_ids'].extend(campus_ids)

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
            document.update({'id': item_id})
            document['sort_title'] = add_sort_title(document)

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
