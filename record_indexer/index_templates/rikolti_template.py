import sys
import json
import requests
import copy
from record_indexer import settings

'''
    Create OpenSearch index template for rikolti
    https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html
    https://www.elastic.co/guide/en/elasticsearch/reference/7.9/indices-component-template.html
'''


def main():
    # legacy solr index: https://harvest-stg.cdlib.org/solr/#/dc-collection/query
    # legacy solr schema: https://github.com/ucldc/solr_api/blob/master/dc-collection/conf/schema.xml
    # solr filter documentation: https://solr.apache.org/guide/8_6/filter-descriptions.html
    # TODO add aliases, version, _meta, priority to record_index_template.json
    # TODO make sort_title a multifield of title?
    record_index_config = json.load(open('record_index_config.json'))
    record_schema = record_index_config['template']['mappings']['properties']

    # child schema == record schema, except without the "children" field
    child_schema = copy.deepcopy(record_schema)
    del child_schema['children']
    record_schema["children"]["properties"] = child_schema

    # create index template
    r = requests.put(
        f"{settings.ENDPOINT}/_index_template/rikolti_template",
        headers={
            "Content-Type": "application/json"
        },
        data=json.dumps(record_index_config),
        auth=settings.AUTH
    )
    r.raise_for_status()
    print(r.text)


if __name__ == "__main__":
    sys.exit(main())
