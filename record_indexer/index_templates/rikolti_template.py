import copy
import json
import sys

import requests

from .. import settings
from .record_index_config import RECORD_INDEX_CONFIG
from ..utils import print_opensearch_error

"""
    Create OpenSearch index template for rikolti
    https://opensearch.org/docs/2.3/opensearch/index-templates/
"""


def main():
    # legacy solr index: https://harvest-stg.cdlib.org/solr/#/dc-collection/query
    # legacy solr schema: https://github.com/ucldc/solr_api/blob/master/dc-collection/conf/schema.xml
    # solr filter documentation: https://solr.apache.org/guide/8_6/filter-descriptions.html
    # TODO add aliases, version, _meta, priority to record_index_template.json
    # TODO make sort_title a multifield of title?
    record_schema = RECORD_INDEX_CONFIG["template"]["mappings"]["properties"]

    # child schema == record schema, except without the "children" field
    child_schema = copy.deepcopy(record_schema)
    del child_schema["children"]
    del child_schema["rikolti"]

    # create nested alias fields
    del child_schema["collection_id"]
    del child_schema["campus_id"]
    del child_schema["repository_id"]
    child_schema["children.collection_id"] = {
        "path": "children.collection_url",
        "type": "alias"
    }
    child_schema["children.campus_id"] = {
        "path": "children.campus_url",
        "type": "alias"
    }
    child_schema["children.repository_id"] = {
        "path": "children.repository_url",
        "type": "alias"
    }

    record_schema["children"]["properties"] = child_schema

    # create index template
    url = f"{settings.ENDPOINT}/_index_template/rikolti_template"
    r = requests.put(
        url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(RECORD_INDEX_CONFIG),
        auth=settings.get_auth(),
        verify=settings.verify_certs()
    )
    if not (200 <= r.status_code <= 299):
        print_opensearch_error(r, url)
        r.raise_for_status()
    print(r.text)


if __name__ == "__main__":
    sys.exit(main())
