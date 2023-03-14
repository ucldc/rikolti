from lambda_function import run_indexer
import json

nuxeo_test = {
    'collection_id': 26147
}

run_indexer(json.dumps(nuxeo_test))
