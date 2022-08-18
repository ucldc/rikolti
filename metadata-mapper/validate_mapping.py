import sys
import os
import boto3
import requests
import json
from mapper import Mapper
from lambda_function import get_mapper
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


def validate_mapped_page(collection_id, page_filename):
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

    enrichment_fields = [
        'url_item', 
        'structmap_url', 
        'harvest_id_s', 
        'sort_title', 
        'campus_name', 
        'facet_decade', 
        'campus_data', 
        'repository_url', 
        'collection_url', 
        'repository_name', 
        'reference_image_md5', 
        'repository_data',
        'collection_data',
        'campus_url',
        'collection_name',
        '_version_',
        'timestamp',
        'sort_collection_data',
        'reference_image_dimensions',
        'sort_date_end',
        'sort_date_start'
    ]

    mapped_metadata = json.loads(mapped_metadata)
    for rikolti_record in mapped_metadata:
        # print(f"-----{rikolti_record['calisphere-id']} - {rikolti_record['type']}-----")
        query = {"q": rikolti_record['calisphere-id']}
        solr_record = SOLR(**query)
        for field, value in solr_record.items():
            if field in enrichment_fields or field[-3:] == "_ss":
                continue
            if field not in rikolti_record:
                print(
                    f"{rikolti_record['calisphere-id']} :::: "
                    f"{field} :::: {value} :::: "
                    f"NONE"
                )
                continue
            if rikolti_record[field] != value:
                print(
                    f"{rikolti_record['calisphere-id']} :::: "
                    f"{field} :::: {value} :::: "
                    f"{rikolti_record[field]}"
                )


def validate_mapped_collection(payload):
    if DEBUG:
        payload = json.loads(payload)

    mapper = get_mapper(payload)
    validate_mapped_page(mapper.collection_id, mapper.page_filename)
    
    next_page = mapper.increment()
    while(next_page):
        validate_mapped_page(
            next_page.get('collection_id'), 
            next_page.get('page_filename')
        )
        next_page = mapper.increment()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate mapped metadata against SOLR")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    validate_mapped_collection(args.payload, {})