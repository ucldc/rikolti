import sys, os
import json
import boto3
import botocore
import requests
from requests_aws4auth import AWS4Auth

'''
    Create an empty index with an explicit mapping (schema)
'''

def main():
    properties = get_properties()

    # set calisphere_id as document _id - needs to be passed in explicitly
    # use strict (vs false) setting to throw an exception if an unknown field is encountered?

    # CREATE EMPTY INDEX
    endpoint = os.environ.get('RIKOLTI_ES_ENDPOINT')
    auth = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

    index_name = 'test'
    url = os.path.join(endpoint, index_name)
    payload = {
        "mappings": {
            "properties": properties
        }
    }
    headers = {
        "Content-Type": "application/json"
    }

    # delete index if it exists - for testing!!
    r = requests.delete(url, auth=auth)
    r.raise_for_status()

    # create new empty index
    r = requests.put(url, headers=headers, data=json.dumps(payload), auth=auth)
    r.raise_for_status()
    print(r.text)

def get_properties():
    
    # legacy solr index: https://harvest-stg.cdlib.org/solr/#/dc-collection/query
    # legacy solr schema: https://github.com/ucldc/solr_api/blob/master/dc-collection/conf/schema.xml
    # solr filter documentation: https://solr.apache.org/guide/8_6/filter-descriptions.html

    properties = {}

    # fields to be mapped as `text` for full-text search
    text_fields = [
        'title',
        'alternative_title',
        'contributor',
        'coverage',
        'creator',
        'date',
        'description',
        'extent',
        'format',
        'genre',
        'identifier',
        'language',
        'location',
        'provenance',
        'publisher',
        'relation',
        'rights',
        'rights_holder',
        'rights_note',
        'rights_date',
        'source',
        'spatial',
        'subject',
        'temporal',
        'transcription',
        'type',
        #'structmap_text'
    ]

    # keyword fields for exact searching
    keyword_fields = [
        'calisphere_id',
        'harvest_id',
        'campus_name',
        'campus_data',
        'collection_name',
        'collection_data',
        'sort_collection_data',
        'repository_name',
        'repository_data',
        'rights_uri',
        #'url_item',
        #'reference_image_md5',
        #'reference_image_dimensions',
        #'structmap_url',
        #'manifest',
        #'object_template'
    ]

    date_fields = [
        'created',
        'last_modified',
        'sort_date_start',
        'sort_date_end'
    ]

    integer_fields = [
        'campus_id',
        'collection_id',
        'repository_id',
        'item_count'
    ]

    alpha_space_sort_fields = [
        'sort_title'
    ]

    # fields to be additionally mapped as `keyword` fields (aka "multi-fields")
    keyword_multi_fields = [
        'title',
        'alternative_title',
        'contributor',
        'coverage',
        'created',
        'creator',
        'date',
        'extent',
        'format',
        'genre',
        'identifier',
        'language',
        'last_modified',
        'location',
        'publisher',
        'relation',
        'rights',
        'rights_holder',
        'rights_note',
        'rights_date',
        'source',
        'spatial',
        'subject',
        'temporal',
        'type'
    ]

    # store complex objects as nested?

    for tf in text_fields:
        properties[tf] = {
            "type": "text"
        }

    for kf in keyword_fields:
        properties[kf] = {
            "type": "keyword"
        }

    for df in date_fields:
        properties[df] = {
            "type": "date"
        }

    for intf in integer_fields:
        properties[intf] = {
            "type": "integer"
        }

    '''
    for assf in alpha_space_sort_fields:
        properties[assf] = {
            "type": 
        }
    '''

    for kcf in keyword_copy_fields:
        properties[kcf]["fields"] = {
            "raw": {
                "type": "keyword"
            }
        }

    return properties


if __name__ == "__main__":
    sys.exit(main())




