
oai_harvests = [
  { # 2206: Big Pine Citizen Newspaper
    # 19 items
    # extra_data: big-pine-citizen-newspaper
    # curl 'http://oai.quartexcollections.com/lmudigitalcollections/oai2?verb=ListMetadataFormats'
    "collection_id": 2206,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
      "url": "http://oai.quartexcollections.com/lmudigitalcollections/oai2",
      "oai_qs_args": "big-pine-citizen-newspaper"
    }
  }
]

'''
oai_harvests = [
  { # 2206: Big Pine Citizen Newspaper
    # 19 items
    # extra_data: big-pine-citizen-newspaper
    # curl 'http://oai.quartexcollections.com/lmudigitalcollections/oai2?verb=ListMetadataFormats'
    "collection_id": 2206,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
      "url": "http://oai.quartexcollections.com/lmudigitalcollections/oai2",
      "oai_qs_args": "big-pine-citizen-newspaper"
    }
  },
  { # 27435: Sugoroku
    # 155 items
    # extra_data: metadataPrefix=marcxml&set=sugoroku
    'collection_id': 27435,
    'harvest_type': "OAIFetcher",
    'write_page': 0,
    'oai': {
        'url': "https://digicoll.lib.berkeley.edu/oai2d",
        'oai_qs_args': "metadataPrefix=marcxml&set=sugoroku"
    }
  },
  { # 26693: Fritz-Metcalf
    # 8,729 items
    # extra_data: metadataPrefix=marcxml&set=fritz-metcalf
    # curl 'https://digicoll.lib.berkeley.edu/oai2d?verb=ListRecords&metadataPrefix=marcxml' > test_data/fritz-metcalf.xm
    # <resumptionToken completeListSize="46849" cursor="0" expirationDate="2022-10-12T21:50:56Z">___AzfqSX</resumptionToken>
    "collection_id": 26693,
    "harvest_type": "OAIFetcher",
    "write_page": 0, 
    "oai": {
      "url": "https://digicoll.lib.berkeley.edu/oai2d",
      "oai_qs_args": "metadataPrefix=marcxml&set=fritz-metcalf"
    }
  },
  { # 27934: San Francisco Gay Men's Chorus records, 2009-01
    # 47 items
    # extra_data: node:23758
    # 
    "collection_id": 27934,
    "harvest_type": "OAIFetcher",
    "write_page": 0,
    "oai": {
      "url": "https://glbt.i8.dgicloud.com/oai/request",
      "oai_qs_args": "node:23758"
    }
  }
]
'''
