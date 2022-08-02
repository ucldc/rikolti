from lambda_function import lambda_handler
from test_data.nuxeo_harvests import *
import json

harvests = nuxeo_harvests[0:1]
#  + nuxeo_complex_object_harvests + nuxeo_nested_complex_object_harvests
for harvest in harvests:
    print(f"tests.py: {json.dumps(harvest)}")
    lambda_handler(json.dumps(harvest), None)
    print(f"Harvested: {str(harvest)}")