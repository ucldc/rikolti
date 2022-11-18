# Nuxeo Test Harvests
# 27414
# 26710 - nuxeo video
# 9513 - nuxeo image

nuxeo_harvests = [
  { # 26695: Anthill
    "collection_id": 26695,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Anthill/"
    }
  }, 
  { # 26098: Ramicova
      'collection_id': 26098,
      'harvest_type': 'nuxeo',
      'write_page': 0,
      'nuxeo': {
          'path': "/asset-library/UCM/Ramicova/"
      }
  },
  { # 26746: Nordskog Papers
    "collection_id": 26746,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCR/Water Resources Collections & Archives/Andrae B. Nordskog papers"
    }
  },
  { # 26697: Spectrum
    "collection_id": 26697,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Spectrum/"
    }
  },
  { # 27694: Halpern
    "collection_id": 27694,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCR/SCUA/Archival/MS075",
      "fetch_children": True
    }
  },
  { # 27141: Citrus
    "collection_id": 27141,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCR/SCUA/Archival/UA042/published",
      "fetch_children": True
    }
  }
]

nuxeo_complex_object_harvests = [
  { # 466: AIDS
    'collection_id': 466,
    'harvest_type': 'nuxeo',
    'write_page': 0,
    'nuxeo': {
        'path': "/asset-library/UCSF/MSS 2000-31 AIDS Ephemera Collection/"
    }
  },
  { # 27012: UC Cooperative Extension
    "collection_id": 27012,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCM/UCCE/Merced/PUBLISH/"
    }
  },
  { # 68: McLean
    "collection_id": 68,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCM/McLean/Publish/"
    }
  },
  { # 76: Nightingale
    "collection_id": 76,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCM/NightingaleDiaries/",
      "fetch_children": True
    }
  }
]

nuxeo_nested_complex_object_harvests = [
    { # 14256: McDaniel
    "collection_id": 14256,
    "harvest_type": "nuxeo",
    "write_page": 0,
    "nuxeo": {
      "path": "/asset-library/UCM/Wilma_McDaniel/Publish/"
    }
  }
]
