from lambda_function import lambda_handler
import json

nuxeo_test = {
    'collection_id': 466,
    'harvest_type': 'NuxeoFetcher',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCSF/MSS 2000-31 AIDS Ephemera Collection/"
    }
}

ramicova_test = {
    'collection_id': 26098,
    'harvest_type': 'NuxeoFetcher',
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

oac_test = {
    'collection_id': 509,
    'harvest_type': 'oac',
    'write_page': 0,
    'oac': {
        'url': "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1z09n955",
    }
}
nordskogpapers = {
  "collection_id": 26746,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/Special Collections & University Archives/Archival/Andrae B. Nordskog papers/"
  }
}
spectrum = {
  "collection_id": 26697,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Spectrum/"
  }
}

nightingale_test = {
  "collection_id": 76,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/NightingaleDiaries/",
    "fetch_children": True
  }
}

mcdaniel_test = {
  "collection_id": 14256,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/Wilma_McDaniel/Publish/",
    "fetch_children": True
  }
}

halpern_test = {
  "collection_id": 27694,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/SCUA/Archival/MS075",
    "fetch_children": True
  }
}

citrus_test = {
  "collection_id": 27141,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/SCUA/Archival/UA042/published",
    "fetch_children": True
  }
}

oai_test = {
  "collection_id": 27836,
  "harvest_type": "OAIFetcher",
  "write_page": 0,
  "oai": {
    "url": (
      "http://www.adc-exhibits.museum.ucsb.edu/"
      "oai-pmh-repository/request"),
    "set": 38
  }
}

large_oai_test = {
  "collection_id": 26673,
  "harvest_type": "OAIFetcher",
  "write_page": 0,
  "oai": {
    "url": "http://exhibits.sos.ca.gov/oai-pmh-repository/request",
    "set": 1
  }
}

# 27414
# 26710 - nuxeo video
# 9513 - nuxeo image
# lambda_handler(json.dumps(oac_test), {})
# lambda_handler(json.dumps(nuxeo_test), {})
# lambda_handler(json.dumps(ramicova_test), {})
# lambda_handler(json.dumps(nordskogpapers), {})
# lambda_handler(json.dumps(oai_test), {})
# lambda_handler(json.dumps(citrus_test), {})
# lambda_handler(json.dumps(oai_test), {})
lambda_handler(json.dumps(large_oai_test), {})
