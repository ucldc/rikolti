oai_harvests = [
  { # 27435: Sugoroku - metadata_prefix & metadata_set explicitly specified
    'collection_id': 27435,
    'harvest_type': "OAIFetcher",
    'write_page': 0,
    'oai': {
        'url': "https://digicoll.lib.berkeley.edu/oai2d",
        'metadata_prefix': "marcxml",
        'metadata_set': "sugoroku"
    }
  },
  { # 27836: AE Hanson Landscape Designs - metadata_set explicitly specified
    "collection_id": 27836,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
      "url": (
        "http://www.adc-exhibits.museum.ucsb.edu/"
        "oai-pmh-repository/request"),
      "metadata_set": 38
    }
  },
  # { # 26673: Old Series Trademarks - large collection, simple extra_data, API seems to be down
  #   "collection_id": 26673,
  #   "harvest_type": "OAIFetcher",
  #   "write_page": 0,
  #   "oai": {
  #     "url": "http://exhibits.sos.ca.gov/oai-pmh-repository/request",
  #     "extra_data": "1"
  #   }
  # },
  { # 26693: Fritz-Metcalf - complex extra data
    "collection_id": 26796,
    "harvest_type": "OAIFetcher",
    "write_page": 0, 
    "oai": {
      "url": "https://digital.lib.calpoly.edu/oai2/oai.php",
      "extra_data": "metadataPrefix=oai_dc&set=rekl_panamerican-ms159"
    }
  },
  { # 2206: Big Pine Citizen Newspaper - simple extra data
    "collection_id": 2206,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
      "url": "http://oai.quartexcollections.com/lmudigitalcollections/oai2",
      "extra_data": "big-pine-citizen-newspaper"
    }
  }
]


