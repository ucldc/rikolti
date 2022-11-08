from lambda_function import map_page
# from validate_mapping import validate_mapped_collection
from sample_data.nuxeo_harvests import *
from sample_data.oac_harvests import *
from sample_data.islandora_harvests import *
import json

harvests = islandora_harvests

for harvest in harvests:
    print(f"mapping tests: {json.dumps(harvest)}")
    map_page(json.dumps(harvest), {})
    print(f"mapped: {str(harvest)}")

# for harvest in harvests:
#     print(f"validate mapping: {json.dumps(harvest)}")
#     validate_mapped_collection(json.dumps(harvest))
#     print(f"validated: {str(harvest)}")
    