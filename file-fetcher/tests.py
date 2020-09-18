from lambda_function import lambda_handler
import json

nuxeo_local_test = {
    'collection_id': 466,
    'harvest_type': 'nuxeo',
    'metadata_date': '2020-09-14'
}

nuxeo_s3_test = {
    'collection_id': 466,
    'harvest_type': 'nuxeo',
    'metadata_date': '2020-08-27'
}

nuxeo_s3_test_line_2 = {
    'collection_id': 466,
    'harvest_type': 'nuxeo',
    'metadata_date': '2020-08-27',
    'start_page': '0',
    'start_line': 2
}

lambda_handler(json.dumps(nuxeo_s3_test_line_2), {})