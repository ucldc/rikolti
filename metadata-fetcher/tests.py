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

anthill = {
  "collection_id": 26695,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    "path": "/asset-library/UCI/SCA_UniversityArchives/Publications/Anthill/"
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

# max recursion level test
UC_cooperative_extension = {
  "collection_id": 27012,
  "harvest_type": "NuxeoFetcher",
  "write_page": 0,
  "nuxeo": {
    # "path": "/asset-library/UCM/McLean/Publish/" - 68
    "path": "/asset-library/UCM/UCCE/Merced/PUBLISH/"
    # "path": "/asset-library/UCM/Wilma_McDaniel/Publish/" - 14256
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

tudor_engineering = {
  "collection_id": 22973,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt28702559"
  }
}

multipage_oac = {
  "collection_id": 22456,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8pn97ch"
  }
}

large_oac = {
  "collection_id": 25496,
  "harvest_type": "OACFetcher",
  "write_page": 0,
  "oac": {
    'url': 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22'
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

# complex nested
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


# 27414
# 26710 - nuxeo video
# 9513 - nuxeo image
# lambda_handler(json.dumps(oac_test), {})
# lambda_handler(json.dumps(nuxeo_test), {})
# lambda_handler(json.dumps(ramicova_test), {})
# lambda_handler(json.dumps(nordskogpapers), {})
# lambda_handler(json.dumps(oai_test), {})
# lambda_handler(json.dumps(citrus_test), {})
# lambda_handler(json.dumps(tudor_engineering), {})
# lambda_handler(json.dumps(multipage_oac), {})
lambda_handler(json.dumps(mcdaniel_test), {})

# OAC datel harvests
oac_datel_509_alameda_county_1913_views = {
    'collection_id': "509-oac-datel",
    'harvest_type': 'DatelOACFetcher',
    'write_page': 0,
    'oac': {
        'url': "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1z09n955",
    }
}
oac_datel_22973_tudor_engineering = {
  "collection_id": "22973-oac-datel",
  "harvest_type": "DatelOACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt28702559"
  }
}
oac_datel_multipage_22456_tech_and_env_postwar_house_socal = {
  "collection_id": "22456-oac-datel",
  "harvest_type": "DatelOACFetcher",
  "write_page": 0,
  "oac": {
    "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8pn97ch"
  }
}
oac_datel_large_25496_1906_sf_earthquake_and_fire = {
  "collection_id": "25496-oac-datel",
  "harvest_type": "DatelOACFetcher",
  "write_page": 0,
  "oac": {
    'url': 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22'
  }
}

# OAC Json Harvests
oac_json_22973_tudor_engineering = {
  "collection_id": "22973-2",
  "harvest_type": "JsonOAC",
  "write_page": 0,
  "oac": {
    "url": "http://dsc-dsc2-dev.cdlib.org/search?facet=type-tab&style=cui&rmode=json&relation=ark:/13030/kt28702559"
  }
}
oac_json_multipage_22456_tech_and_env_postwar_house_socal = {
  "collection_id": "22456-2",
  "harvest_type": "JsonOAC",
  "write_page": 0,
  "oac": {
    "url": "http://dsc-dsc2-dev.cdlib.org/search?facet=type-tab&style=cui&rmode=json&relation=ark:/13030/c8pn97ch"
  }
}
oac_json_large_25496_1906_sf_earthquake_and_fire = {
  "collection_id": "25496-2",
  "harvest_type": "JsonOAC",
  "write_page": 0,
  "oac": {
    "url": "http://dsc-dsc2-dev.cdlib.org/search?facet=type-tab&style=cui&rmode=json&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22"
  }
}

# OAI Harvests
oai_27435_sugoroku = {
    "collection_id": 27435,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
        "url": "https://digicoll.lib.berkeley.edu/oai2d",
        "metadata_prefix": "marcxml",
        "metadata_set": "sugoroku"
    }
}

oai_26693_complex_extra_data = {
  "collection_id": 26796,
  "harvest_type": "OAIFetcher",
  "write_page": 0, 
  "oai": {
    "url": "https://digital.lib.calpoly.edu/oai2/oai.php",
    "extra_data": "metadataPrefix=oai_dc&set=rekl_panamerican-ms159"
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
    "metadata_set": 38
  }
}

# doesn't work?
# oai_large_26673_old_series_trademarks = {
#   "collection_id": 26673,
#   "harvest_type": "OAIFetcher",
#   "write_page": 0,
#   "oai": {
#     "url": "http://exhibits.sos.ca.gov/oai-pmh-repository/request",
#     "extra_data": 1
#   }
# }

oai_2206 = {
  "collection_id": 2206,
  "harvest_type": "OAIFetcher",
  "write_page": 0,
  "oai": {
    "url": "http://oai.quartexcollections.com/lmudigitalcollections/oai2",
    "extra_data": "big-pine-citizen-newspaper"
  }
}

# lambda_handler(json.dumps(oai_27435_sugoroku), {})
# lambda_handler(json.dumps(oai_26693_complex_extra_data), {})
# lambda_handler(json.dumps(oai_27836_ae_hanson_landscape_designs), {})
# lambda_handler(json.dumps(oai_2206), {})

lambda_handler(json.dumps(oac_datel_22973_tudor_engineering), {})
# lambda_handler(json.dumps(oac_datel_multipage_22456_tech_and_env_postwar_house_socal), {})
# lambda_handler(json.dumps(oac_datel_large_25496_1906_sf_earthquake_and_fire), {})



# oac_datel_tests = [
#     {
#         "collection_id": "17399-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt909nf85q"
#         }
#     },
#     {
#         "collection_id": "17457-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf509nb6gg"
#         }
#     },
#     {
#         "collection_id": "17532-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb367nb1qc"
#         }
#     },
#     {
#         "collection_id": "17534-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb1w100435"
#         }
#     },
#     {
#         "collection_id": "17669-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt95802858"
#         }
#     },
#     {
#         "collection_id": "17964-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3t1nb6b1"
#         }
#     },
#     {
#         "collection_id": "18083-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf6n39p537"
#         }
#     },
#     {
#         "collection_id": "18086-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt000032sk"
#         }
#     },
#     {
#         "collection_id": "18103-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7v19p57x"
#         }
#     },
#     {
#         "collection_id": "18195-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8m3nb7px"
#         }
#     },
#     {
#         "collection_id": "18314-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf829009ns"
#         }
#     },
#     {
#         "collection_id": "18327-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft358004x0"
#         }
#     },
#     {
#         "collection_id": "18352-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4r29q3tz"
#         }
#     },
#     {
#         "collection_id": "18427-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8g50226j"
#         }
#     },
#     {
#         "collection_id": "18433-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7w10129m"
#         }
#     },
#     {
#         "collection_id": "18434-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf6t1nb619"
#         }
#     },
#     {
#         "collection_id": "18436-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4199q92w"
#         }
#     },
#     {
#         "collection_id": "18478-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0v19n4gf"
#         }
#     },
#     {
#         "collection_id": "18560-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7s2010k4"
#         }
#     },
#     {
#         "collection_id": "18689-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf367nb064"
#         }
#     },
#     {
#         "collection_id": "18719-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8f59p055"
#         }
#     },
#     {
#         "collection_id": "18772-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0b69n4cm"
#         }
#     },
#     {
#         "collection_id": "18781-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt9d5nd53d"
#         }
#     },
#     {
#         "collection_id": "18788-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft6p3004n2"
#         }
#     },
#     {
#         "collection_id": "18793-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8b69q7nx"
#         }
#     },
#     {
#         "collection_id": "19071-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5d5nc93n"
#         }
#     },
#     {
#         "collection_id": "19107-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt758036nc"
#         }
#     },
#     {
#         "collection_id": "19134-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5n39p42x"
#         }
#     },
#     {
#         "collection_id": "19138-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5z09p42w"
#         }
#     },
#     {
#         "collection_id": "19149-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2n39n8mr"
#         }
#     },
#     {
#         "collection_id": "19161-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5h4nb721"
#         }
#     },
#     {
#         "collection_id": "19162-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb4489p2q6"
#         }
#     },
#     {
#         "collection_id": "19172-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7d5nb5pb"
#         }
#     },
#     {
#         "collection_id": "19174-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7p30102q"
#         }
#     },
#     {
#         "collection_id": "19199-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf338n99v6"
#         }
#     },
#     {
#         "collection_id": "19202-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft400005g9"
#         }
#     },
#     {
#         "collection_id": "19208-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf68700559"
#         }
#     },
#     {
#         "collection_id": "19223-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8j49q750"
#         }
#     },
#     {
#         "collection_id": "19230-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0p300793"
#         }
#     },
#     {
#         "collection_id": "19241-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2199n8c6"
#         }
#     },
#     {
#         "collection_id": "19299-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt396nc6jn"
#         }
#     },
#     {
#         "collection_id": "19312-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt7k4023tp"
#         }
#     },
#     {
#         "collection_id": "19331-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt687022h8"
#         }
#     },
#     {
#         "collection_id": "19333-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4489p33c"
#         }
#     },
#     {
#         "collection_id": "19375-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt6v19p65d"
#         }
#     },
#     {
#         "collection_id": "19390-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8p3009t9"
#         }
#     },
#     {
#         "collection_id": "19393-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt867nc9t4"
#         }
#     },
#     {
#         "collection_id": "19405-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4q2nc7bz"
#         }
#     },
#     {
#         "collection_id": "19406-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf600004r6"
#         }
#     },
#     {
#         "collection_id": "19417-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt0779q1x1"
#         }
#     },
#     {
#         "collection_id": "19536-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9h4nb7rm"
#         }
#     },
#     {
#         "collection_id": "19580-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9t1nb4j1"
#         }
#     },
#     {
#         "collection_id": "19667-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4g50333p"
#         }
#     },
#     {
#         "collection_id": "19802-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8x0nb9rw"
#         }
#     },
#     {
#         "collection_id": "19822-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1q2nb3wk"
#         }
#     },
#     {
#         "collection_id": "19850-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5b69q5bc"
#         }
#     },
#     {
#         "collection_id": "19853-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt096nc9xv"
#         }
#     },
#     {
#         "collection_id": "19967-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5t1nd3bf"
#         }
#     },
#     {
#         "collection_id": "19968-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt700038r4"
#         }
#     },
#     {
#         "collection_id": "20008-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8m3nb687"
#         }
#     },
#     {
#         "collection_id": "20020-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9580070z"
#         }
#     },
#     {
#         "collection_id": "20118-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf409n99xw"
#         }
#     },
#     {
#         "collection_id": "20224-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb9290139g"
#         }
#     },
#     {
#         "collection_id": "20242-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0h4n9810"
#         }
#     },
#     {
#         "collection_id": "20412-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3b69n8z8"
#         }
#     },
#     {
#         "collection_id": "20440-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3199n95w"
#         }
#     },
#     {
#         "collection_id": "20456-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt3199s113"
#         }
#     },
#     {
#         "collection_id": "20535-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3s2007rd"
#         }
#     },
#     {
#         "collection_id": "20551-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2h4nb66p"
#         }
#     },
#     {
#         "collection_id": "20564-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2m3nb209"
#         }
#     },
#     {
#         "collection_id": "20587-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf938nb3z5"
#         }
#     },
#     {
#         "collection_id": "20607-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8489q5km"
#         }
#     },
#     {
#         "collection_id": "20746-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8290242t"
#         }
#     },
#     {
#         "collection_id": "20753-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0h4n97nt"
#         }
#     },
#     {
#         "collection_id": "20775-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb7r29p557"
#         }
#     },
#     {
#         "collection_id": "20808-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf809nb6ph"
#         }
#     },
#     {
#         "collection_id": "20882-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1779n7qw"
#         }
#     },
#     {
#         "collection_id": "20899-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4489p11c"
#         }
#     },
#     {
#         "collection_id": "20923-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt787023c0"
#         }
#     },
#     {
#         "collection_id": "21651-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8t1nd149"
#         }
#     },
#     {
#         "collection_id": "21717-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4k4007qw"
#         }
#     },
#     {
#         "collection_id": "21718-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8199p3jp"
#         }
#     },
#     {
#         "collection_id": "21815-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf6b69p13b"
#         }
#     },
#     {
#         "collection_id": "21830-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8j49p5w3"
#         }
#     },
#     {
#         "collection_id": "22097-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8rj4gwt"
#         }
#     },
#     {
#         "collection_id": "22148-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf258001r8"
#         }
#     },
#     {
#         "collection_id": "22164-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf887006t7"
#         }
#     },
#     {
#         "collection_id": "22220-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt3m3nc60b"
#         }
#     },
#     {
#         "collection_id": "22280-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb1z09n6x3"
#         }
#     },
#     {
#         "collection_id": "22315-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf696nb4sf"
#         }
#     },
#     {
#         "collection_id": "22316-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3d5nb5bx"
#         }
#     },
#     {
#         "collection_id": "22317-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5n39p3mq"
#         }
#     },
#     {
#         "collection_id": "22318-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3w1006n1"
#         }
#     },
#     {
#         "collection_id": "22319-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3c6007c3"
#         }
#     },
#     {
#         "collection_id": "22322-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9m3nb870"
#         }
#     },
#     {
#         "collection_id": "22351-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8h4nf6q0"
#         }
#     },
#     {
#         "collection_id": "22392-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8cr5txf"
#         }
#     },
#     {
#         "collection_id": "22456-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8pn97ch"
#         }
#     },
#     {
#         "collection_id": "22497-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2q2n99qr"
#         }
#     },
#     {
#         "collection_id": "22609-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt1v19n82t"
#         }
#     },
#     {
#         "collection_id": "22725-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft838nb5kh"
#         }
#     },
#     {
#         "collection_id": "22828-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9g5009fm"
#         }
#     },
#     {
#         "collection_id": "22916-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4c600848"
#         }
#     },
#     {
#         "collection_id": "22923-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8765cr2"
#         }
#     },
#     {
#         "collection_id": "22973-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt28702559"
#         }
#     },
#     {
#         "collection_id": "22978-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt7g50386k"
#         }
#     },
#     {
#         "collection_id": "23010-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5s20213j"
#         }
#     },
#     {
#         "collection_id": "23065-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134"
#         }
#     },
#     {
#         "collection_id": "23066-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft6k4007pc"
#         }
#     },
#     {
#         "collection_id": "23070-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt129033hb"
#         }
#     },
#     {
#         "collection_id": "23109-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt7s2036zz"
#         }
#     },
#     {
#         "collection_id": "23141-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8x0nb5rz"
#         }
#     },
#     {
#         "collection_id": "23187-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb6b69p5ff"
#         }
#     },
#     {
#         "collection_id": "23343-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c80z71p6"
#         }
#     },
#     {
#         "collection_id": "23348-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4199q9np"
#         }
#     },
#     {
#         "collection_id": "23384-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8p26wk2"
#         }
#     },
#     {
#         "collection_id": "23397-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt4h4nc7pt"
#         }
#     },
#     {
#         "collection_id": "23568-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt6r29r1w3"
#         }
#     },
#     {
#         "collection_id": "23585-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt409nb02d"
#         }
#     },
#     {
#         "collection_id": "23599-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf638nb54s"
#         }
#     },
#     {
#         "collection_id": "23600-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3b69p0n6"
#         }
#     },
#     {
#         "collection_id": "23641-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2h4nb6jv"
#         }
#     },
#     {
#         "collection_id": "23659-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf487006ds"
#         }
#     },
#     {
#         "collection_id": "23661-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt300019nx"
#         }
#     },
#     {
#         "collection_id": "23662-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt1g5025sm"
#         }
#     },
#     {
#         "collection_id": "23823-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb4r29p36v"
#         }
#     },
#     {
#         "collection_id": "23837-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9x0nb4vb"
#         }
#     },
#     {
#         "collection_id": "23847-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt9c602931"
#         }
#     },
#     {
#         "collection_id": "23924-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4w1004h9"
#         }
#     },
#     {
#         "collection_id": "24072-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt0t1nc9x4"
#         }
#     },
#     {
#         "collection_id": "24123-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf596nb4h0"
#         }
#     },
#     {
#         "collection_id": "24185-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf167nb4rq"
#         }
#     },
#     {
#         "collection_id": "24187-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6b56kh8"
#         }
#     },
#     {
#         "collection_id": "24188-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt809nd1td"
#         }
#     },
#     {
#         "collection_id": "24192-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9r29p4p4"
#         }
#     },
#     {
#         "collection_id": "24199-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5b69p3h6"
#         }
#     },
#     {
#         "collection_id": "24202-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5z09n83w"
#         }
#     },
#     {
#         "collection_id": "24203-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2x0nb5r3"
#         }
#     },
#     {
#         "collection_id": "24204-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5q2nb80s"
#         }
#     },
#     {
#         "collection_id": "24205-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8p3009b2"
#         }
#     },
#     {
#         "collection_id": "24206-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf6r29p4m5"
#         }
#     },
#     {
#         "collection_id": "24207-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf6b69n89c"
#         }
#     },
#     {
#         "collection_id": "24208-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf82900695"
#         }
#     },
#     {
#         "collection_id": "24209-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5580068c"
#         }
#     },
#     {
#         "collection_id": "24210-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf496nb60w"
#         }
#     },
#     {
#         "collection_id": "24211-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb687006v4"
#         }
#     },
#     {
#         "collection_id": "24212-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf458008b2"
#         }
#     },
#     {
#         "collection_id": "24213-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb9n39p2s2"
#         }
#     },
#     {
#         "collection_id": "24214-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb6489p0j8"
#         }
#     },
#     {
#         "collection_id": "24215-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3r29p0j8"
#         }
#     },
#     {
#         "collection_id": "24216-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb6m3nb3nv"
#         }
#     },
#     {
#         "collection_id": "24217-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf32900882"
#         }
#     },
#     {
#         "collection_id": "24218-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf4p300915"
#         }
#     },
#     {
#         "collection_id": "24219-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8f59p1d8"
#         }
#     },
#     {
#         "collection_id": "24220-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5n39p1xd"
#         }
#     },
#     {
#         "collection_id": "24221-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf68701128"
#         }
#     },
#     {
#         "collection_id": "24222-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3c60095z"
#         }
#     },
#     {
#         "collection_id": "24223-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0199n8hn"
#         }
#     },
#     {
#         "collection_id": "24224-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2x0nb5nj"
#         }
#     },
#     {
#         "collection_id": "24225-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb038n99v8"
#         }
#     },
#     {
#         "collection_id": "24259-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0r29n4nc"
#         }
#     },
#     {
#         "collection_id": "24429-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt438nf2mn"
#         }
#     },
#     {
#         "collection_id": "24434-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt1k4033vs"
#         }
#     },
#     {
#         "collection_id": "24439-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt258034d7"
#         }
#     },
#     {
#         "collection_id": "24440-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt2870359t"
#         }
#     },
#     {
#         "collection_id": "24441-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5s2036cj"
#         }
#     },
#     {
#         "collection_id": "24445-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt658037kh"
#         }
#     },
#     {
#         "collection_id": "24484-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf9r29p4fh"
#         }
#     },
#     {
#         "collection_id": "24518-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt2199q0t9"
#         }
#     },
#     {
#         "collection_id": "24613-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt1b69q44j"
#         }
#     },
#     {
#         "collection_id": "24760-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt329002rr"
#         }
#     },
#     {
#         "collection_id": "24981-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5d5nb77z"
#         }
#     },
#     {
#         "collection_id": "25149-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf1870077r"
#         }
#     },
#     {
#         "collection_id": "25152-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8k40079x"
#         }
#     },
#     {
#         "collection_id": "25158-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5f59p1fv"
#         }
#     },
#     {
#         "collection_id": "25195-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf5d5nb2wc"
#         }
#     },
#     {
#         "collection_id": "25229-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8ng4pcz"
#         }
#     },
#     {
#         "collection_id": "25236-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf258005r6"
#         }
#     },
#     {
#         "collection_id": "25252-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt8x0nd5m4"
#         }
#     },
#     {
#         "collection_id": "25267-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf496nb393"
#         }
#     },
#     {
#         "collection_id": "25321-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt0s2017hn"
#         }
#     },
#     {
#         "collection_id": "25471-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf796nb2t9"
#         }
#     },
#     {
#         "collection_id": "25496-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22"
#         }
#     },
#     {
#         "collection_id": "25500-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf196nb4nt"
#         }
#     },
#     {
#         "collection_id": "25503-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf7d5nb8zx"
#         }
#     },
#     {
#         "collection_id": "25507-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf3z09n6w6"
#         }
#     },
#     {
#         "collection_id": "25529-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt6z09q2d6"
#         }
#     },
#     {
#         "collection_id": "25597-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6v69jf3"
#         }
#     },
#     {
#         "collection_id": "25900-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c89g5q2m"
#         }
#     },
#     {
#         "collection_id": "26135-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb1h4nb4h2"
#         }
#     },
#     {
#         "collection_id": "26137-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf8290068n"
#         }
#     },
#     {
#         "collection_id": "26138-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2x0n99n1"
#         }
#     },
#     {
#         "collection_id": "26145-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt68703482"
#         }
#     },
#     {
#         "collection_id": "26191-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7c1"
#         }
#     },
#     {
#         "collection_id": "26193-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb0x0nb4dt"
#         }
#     },
#     {
#         "collection_id": "26194-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k66d5stk/"
#         }
#     },
#     {
#         "collection_id": "26204-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb1t1nb4jj"
#         }
#     },
#     {
#         "collection_id": "26205-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb467nb6mj"
#         }
#     },
#     {
#         "collection_id": "26206-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb0x0nb4gv"
#         }
#     },
#     {
#         "collection_id": "26207-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb367nb61f"
#         }
#     },
#     {
#         "collection_id": "26208-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6jq10tr"
#         }
#     },
#     {
#         "collection_id": "26298-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k65h7g2g"
#         }
#     },
#     {
#         "collection_id": "26299-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt9s2024w4"
#         }
#     },
#     {
#         "collection_id": "26327-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6959hdx"
#         }
#     },
#     {
#         "collection_id": "26388-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6pg1rj3"
#         }
#     },
#     {
#         "collection_id": "26390-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6w095rt"
#         }
#     },
#     {
#         "collection_id": "26396-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=bancroft.berkeley.edu%2Fcollections%2Fcalcultures.html"
#         }
#     },
#     {
#         "collection_id": "26404-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb1x0nb4xw"
#         }
#     },
#     {
#         "collection_id": "26406-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6dz084w"
#         }
#     },
#     {
#         "collection_id": "26472-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6r78f1h/"
#         }
#     },
#     {
#         "collection_id": "26473-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6mg7pb7/"
#         }
#     },
#     {
#         "collection_id": "26655-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=docs%20--%20jarda"
#         }
#     },
#     {
#         "collection_id": "26656-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=fsm&type=text"
#         }
#     },
#     {
#         "collection_id": "26657-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=oh%20--%20jarda&publisher=Regional+Oral+History+Office"
#         }
#     },
#     {
#         "collection_id": "26658-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=roho"
#         }
#     },
#     {
#         "collection_id": "26659-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8qc085c"
#         }
#     },
#     {
#         "collection_id": "26660-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=coph.fullerton.edu%2Fcollections%2FJAcollections.php"
#         }
#     },
#     {
#         "collection_id": "26661-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=www.janm.org%2Fnrc"
#         }
#     },
#     {
#         "collection_id": "26662-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=www.library.ucsb.edu%2Fspecial-collections%2Fcema%2Fchicano-latino-collections"
#         }
#     },
#     {
#         "collection_id": "26663-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=www.lib.berkeley.edu%2FEAL%2Fstone%2Findex.html"
#         }
#     },
#     {
#         "collection_id": "26664-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=clarklibrary.ucla.edu%2Fcollections%2Fmontana"
#         }
#     },
#     {
#         "collection_id": "26666-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=library.ucsc.edu%2Fregional-history-project"
#         }
#     },
#     {
#         "collection_id": "26780-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5p3019m2&publisher=%22california+historical+society%22"
#         }
#     },
#     {
#         "collection_id": "26781-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5p3019m2&publisher=%22ethnic+studies+library%22"
#         }
#     },
#     {
#         "collection_id": "26783-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt5p3019m2&publisher=%22oroville+chinese+temple%22"
#         }
#     },
#     {
#         "collection_id": "26786-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22california+historical+society%22"
#         }
#     },
#     {
#         "collection_id": "26787-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22california+state+library%22"
#         }
#     },
#     {
#         "collection_id": "26788-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22huntington+library%22"
#         }
#     },
#     {
#         "collection_id": "26789-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22society+of+california+pioneers%22"
#         }
#     },
#     {
#         "collection_id": "26790-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22stanford+university+library%22"
#         }
#     },
#     {
#         "collection_id": "26916-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/ft2q2n98nm"
#         }
#     },
#     {
#         "collection_id": "26947-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://oac.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=oskicat.berkeley.edu&subject=Voices%20in%20Confinement;publisher=The+Bancroft+Library"
#         }
#     },
#     {
#         "collection_id": "26948-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&identifier=%22G4361.E1%201942%20.C3%22"
#         }
#     },
#     {
#         "collection_id": "26953-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6qf8th3/"
#         }
#     },
#     {
#         "collection_id": "26954-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6xs5w3q/"
#         }
#     },
#     {
#         "collection_id": "26958-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c89p36cr"
#         }
#     },
#     {
#         "collection_id": "26970-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb3p3008zb/"
#         }
#     },
#     {
#         "collection_id": "26971-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb9n39p6rg/"
#         }
#     },
#     {
#         "collection_id": "26972-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb700011x3/"
#         }
#     },
#     {
#         "collection_id": "27026-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k61r6qbr/"
#         }
#     },
#     {
#         "collection_id": "27082-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb2s2008s3/"
#         }
#     },
#     {
#         "collection_id": "27083-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb7489p4k1/"
#         }
#     },
#     {
#         "collection_id": "27084-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5h4nb763/"
#         }
#     },
#     {
#         "collection_id": "27139-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6kk9kft"
#         }
#     },
#     {
#         "collection_id": "27157-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6t72h8t"
#         }
#     },
#     {
#         "collection_id": "27195-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6z326nw"
#         }
#     },
#     {
#         "collection_id": "27207-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6mc975b"
#         }
#     },
#     {
#         "collection_id": "27326-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8sq966g"
#         }
#     },
#     {
#         "collection_id": "27406-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k62r3rg4"
#         }
#     },
#     {
#         "collection_id": "27431-oac-datel",
#         "harvest_type": "DatelOACFetcher",
#         "write_page": 0,
#         "oac": {
#             "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/k6pn966r"
#         }
#     }
# ]

# for oac_datel_test in oac_datel_tests:
#   lambda_handler(json.dumps(oac_datel_test), {})
