from lambda_function import lambda_handler
import json

# Nuxeo Harvests
nuxeo_466_AIDS = {
    'collection_id': 466,
    'harvest_type': 'NuxeoFetcher',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCSF/MSS 2000-31 AIDS Ephemera Collection/"
    }
}
nuxeo_26695_anthill = {
  "collection_id": 26695,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Anthill/"
  }
}
nuxeo_26098_ramicova = {
    'collection_id': 26098,
    'harvest_type': 'NuxeoFetcher',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCM/Ramicova/"
    }
}
nuxeo_26746_nordskogpapers = {
  "collection_id": 26746,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/Special Collections & University Archives/Archival/Andrae B. Nordskog papers/"
  }
}
nuxeo_26697_spectrum = {
  "collection_id": 26697,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Spectrum/"
  }
}
nuxeo_27694_halpern = {
  "collection_id": 27694,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/SCUA/Archival/MS075",
    "fetch_children": True
  }
}
nuxeo_27141_citrus = {
  "collection_id": 27141,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCR/SCUA/Archival/UA042/published",
    "fetch_children": True
  }
}

# More Nuxeo Samples
# 27414
# 26710 - nuxeo video
# 9513 - nuxeo image

# Nuxeo Complex Object Harvests
nuxeo_complex_27012_uc_cooperative_extension = {
  "collection_id": 27012,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/UCCE/Merced/PUBLISH/"
  }
}
nuxeo_complex_68_mclean = {
  "collection_id": 68,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/McLean/Publish/"
  }
}
nuxeo_76_nightingale = {
  "collection_id": 76,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/NightingaleDiaries/",
    "fetch_children": True
  }
}

# Nuxeo Complex Nested Harvests
nuxeo_complex_14256_mcdaniel = {
  "collection_id": 14256,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCM/Wilma_McDaniel/Publish/"
  }
}

# OAC Harvests
oac_509_alameda_county_1913_views = {
    'collection_id': 509,
    'harvest_type': 'oac',
    'write_page': 0,
    'oac': {
        'url': "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1z09n955",
    }
}
oac_22973_tudor_engineering = {
  "collection_id": 22973,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt28702559"
  }
}
oac_multipage_22456_tech_and_env_postwar_house_socal = {
  "collection_id": 22456,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8pn97ch"
  }
}
oac_large_25496_1906_sf_earthquake_and_fire = {
  "collection_id": 25496,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    'url': 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22'
  }
}

# OAI Harvests
oai_27435_sugoroku = {
    'collection_id': 27435,
    'harvest_type': 'oai',
    'write_page': 0,
    'oai': {
        'url': "https://digicoll.lib.berkeley.edu/oai2d",
        'metadataPrefix': "marcxml",
        'oai_set': "sugoroku"
    }
}
oai_27836_ae_hanson_landscape_designs = {
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
oai_large_26673_old_series_trademarks = {
  "collection_id": 26673,
  "harvest_type": "OAIFetcher",
  "write_page": 0,
  "oai": {
    "url": "http://exhibits.sos.ca.gov/oai-pmh-repository/request",
    "set": 1
  }
}

# lambda_handler(json.dumps(oai_27836_ae_hanson_landscape_designs), {})
lambda_handler(json.dumps(oai_large_26673_old_series_trademarks), {})
