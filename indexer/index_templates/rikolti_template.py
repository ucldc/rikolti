import sys, os
import json
import boto3
import botocore
import requests
import copy

'''
    Create OpenSearch index template for rikolti
    https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html
    https://www.elastic.co/guide/en/elasticsearch/reference/7.9/indices-component-template.html
'''

ENDPOINT = os.environ.get('RIKOLTI_ES_ENDPOINT')
AUTH = ('rikolti', os.environ.get('RIKOLTI_ES_PASS'))

def main():

    properties = get_properties()

    # TODO add aliases, version, _meta, priority
    payload = {
        "index_patterns": ["rikolti*"],
        "template": {
            "settings": {
                "number_of_shards": 1,
                "analysis": {
                    "analyzer": {
                        "keyword_lowercase_trim": {
                            "tokenizer": "keyword",
                            "filter": ["trim", "lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": properties
            }
        }
    }

    # create the API request
    url = os.path.join(ENDPOINT, "_index_template/rikolti_template")

    headers = {
        "Content-Type": "application/json"
    }

    # create index template
    r = requests.put(url, headers=headers, data=json.dumps(payload), auth=AUTH)
    r.raise_for_status()
    print(r.text)


def get_properties():
    
    # legacy solr index: https://harvest-stg.cdlib.org/solr/#/dc-collection/query
    # legacy solr schema: https://github.com/ucldc/solr_api/blob/master/dc-collection/conf/schema.xml
    # solr filter documentation: https://solr.apache.org/guide/8_6/filter-descriptions.html

    properties = {}

    # `text` fields for full-text search
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
        'type'
    ]

    # `keyword` fields for exact searching
    keyword_fields = [
        'calisphere-id',
        'id',
        'campus_name',
        'campus_data',
        'collection_name',
        'collection_data',
        'collection_url',
        'sort_collection_data',
        'repository_name',
        'repository_data',
        'repository_url',
        'rights_uri',
        'reference_image_md5',
        'reference_image_dimensions',
        'manifest',
        'object_template',
        'url_item'
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


    for f in text_fields:
        properties[f] = {
            "type": "text"
        }

    for f in keyword_fields:
        properties[f] = {
            "type": "keyword"
        }

    for f in date_fields:
        properties[f] = {
            "type": "date"
        }

    for f in integer_fields:
        properties[f] = {
            "type": "integer"
        }

    properties["media"] = {
        "properties": {
            "media_filepath": {"type": "keyword"},
            "mimetype": {"type": "keyword"}
        }
    }

    properties["media_source"] = {
        "properties": {
            "filename": {"type": "keyword"},
            "mimetype": {"type": "keyword"},
            "nuxeo_type": {"type": "keyword"},
            "url": {"type": "keyword"}
        }
    }

    properties["thumbnail"] = {
        "properties": {
            "mimetype": {"type": "keyword"},
            "thumbnail_filepath": {"type": "keyword"}
        }
    }

    properties["thumbnail_source"] = {
        "properties": {
            "filename": {"type": "keyword"},
            "mimetype": {"type": "keyword"},
            "nuxeo_type": {"type": "keyword"},
            "url": {"type": "keyword"}
        }
    }

    # create multifield of type keyword
    for f in keyword_multi_fields:
        properties[f]["fields"] = {
            "raw": {
                "type": "keyword"
            }
        }

    # TODO make this a multifield for `title` field?
    properties["sort_title"] = {
        "type": "text",
        "analyzer": "keyword_lowercase_trim"
    }

    # add children as nested field
    # children have the same properties/schema as the parent object
    properties["children"] = {
        "type": "nested",
        "properties": copy.deepcopy(properties)
    }

    return properties


if __name__ == "__main__":
    sys.exit(main())




