from lambda_function import fetch_collection
from sample_data.nuxeo_harvests import nuxeo_harvests, \
    nuxeo_complex_object_harvests, nuxeo_nested_complex_object_harvests
from sample_data.oac_harvests import oac_harvests
from sample_data.oai_harvests import oai_harvests
import json

harvests = [
    # nuxeo_harvests[1], nuxeo_complex_object_harvests[0],
    # nuxeo_nested_complex_object_harvests[0],
    oac_harvests[0], oai_harvests[0]
]

for harvest in harvests:
    print(f"tests.py: {json.dumps(harvest)}")
    status = fetch_collection(json.dumps(harvest), {})
    print(f"Harvest status: {status}")
