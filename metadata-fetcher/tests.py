from lambda_function import lambda_handler
import json

nuxeo_test = {
    'collection_id': 466,
    'harvest_type': 'nuxeo',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCSF/MSS 2000-31 AIDS Ephemera Collection/"
    }
}

ramicova_test = {
    'collection_id': 26098,
    'harvest_type': 'nuxeo',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCM/Ramicova/"
    }
}

oai_test = {
    'collection_id': 27435,
    'harvest_type': 'oai',
    'write_page': 0,
    'oai': {
        'url': "https://digicoll.lib.berkeley.edu/oai2d",
        'metadataPrefix': "marcxml",
        'oai_set': "sugoroku"
    }
}

lambda_handler(json.dumps(nuxeo_test), {})
# lambda_handler(json.dumps(ramicova_test), {})
# lambda_handler(json.dumps(oai_test), {})