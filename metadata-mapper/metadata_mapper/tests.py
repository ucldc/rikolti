from lambda_function import lambda_handler
from validate_mapping import validate_mapped_collection
from test_data.nuxeo_harvests import *
from test_data.oac_harvests import *
from test_data.islandora_harvests import *
import json

harvests = islandora_harvests

for harvest in harvests:
    print(f"mapping tests: {json.dumps(harvest)}")
    lambda_handler(json.dumps(harvest), {})
    print(f"mapped: {str(harvest)}")

# for harvest in harvests:
#     print(f"validate mapping: {json.dumps(harvest)}")
#     validate_mapped_collection(json.dumps(harvest))
#     print(f"validated: {str(harvest)}")
    