from lambda_function import lambda_handler
from test_data.nuxeo_harvests import *
from test_data.oac_harvests import *
from test_data.oai_harvests import *
import json

harvests = nuxeo_harvests
#  + nuxeo_complex_object_harvests + nuxeo_nested_complex_object_harvests
for harvest in harvests:
    print(f"tests.py: {json.dumps(harvest)}")
    lambda_handler(json.dumps(harvest), {})
    print(f"Harvested: {str(harvest)}")