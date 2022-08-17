import sys
import os
import boto3
import requests
import json
from mapper import Mapper
DEBUG = os.environ.get('DEBUG', False)
SOLR_URL = os.environ.get('UCLDC_SOLR_URL', False)
SOLR_API_KEY = os.environ.get('UCLDC_SOLR_API_KEY', False)

def SOLR(**params):
    solr_url = f'{SOLR_URL}/query/'
    solr_auth = {'X-Authentication-Token': SOLR_API_KEY}
    query = {}
    for key, value in list(params.items()):
        key = key.replace('_', '.')
        query.update({key: value})
    res = requests.post(solr_url, headers=solr_auth, data=query, verify=False)
    res.raise_for_status()
    results = json.loads(res.content.decode('utf-8'))
    facet_counts = results.get('facet_counts', {})
    return results['response']['docs'][0]


def validate_mapped(collection_id, page_filename):
    if DEBUG:
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir, 
            'rikolti_bucket', 
            'mapped_metadata', 
            str(collection_id),
        ])
        page_path = os.sep.join([local_path, str(page_filename)])
        page = open(page_path, "r")
        mapped_metadata = page.read()
    else:
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{self.collection_id}/{page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        mapped_metadata = s3_obj_summary['Body'].read()

    mapped_metadata = json.loads(mapped_metadata)
    for rikolti_record in mapped_metadata:
        print(f"-----{rikolti_record['calisphere-id']} - {rikolti_record['type']}-----")
        query = {"q": rikolti_record['calisphere-id']}
        solr_record = SOLR(**query)
        for field, value in solr_record.items():
            if field in ['url_item', 'structmap_url', 'harvest_id_s', 'sort_title', 'campus_name', 'facet_decade', 'campus_data', 'repository_url', 'collection_url', 'repository_name', 'reference_image_md5', 'repository_data'] or field[-3:] == "_ss":
                continue
            if field not in rikolti_record:
                print(
                    f"{field} - doesn't exist - "
                    f"target: {value}"
                )
                continue
            if rikolti_record[field] != value:
                print(
                    f"{field} - "
                    f"target: {value} - "
                    f"actual: {rikolti_record[field]}"
                )


    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validated mapped metadata against SOLR")
    parser.add_argument('collection_id', help='collection id')
    parser.add_argument('page_filename', help='page filename')
    args = parser.parse_args(sys.argv[1:])
    validate_mapped(args.collection_id, args.page_filename)
