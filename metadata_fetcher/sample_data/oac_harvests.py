oac_harvests = [
  { # 22973: Tudor Engineering
    "collection_id": 22973,
    "harvest_type": "OACFetcher",
    "write_page": 0,
    "oac": {
      "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/kt28702559"
    }
  },
  { # 22456: Tech and Env Postwar House SoCal
    "collection_id": 22456,
    "harvest_type": "OACFetcher",
    "write_page": 0,
    "oac": {
      "url": "http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/c8pn97ch"
    }
  },
  { # 25496: 1906 San Francisco Earthquake and Fire
    "collection_id": 25496,
    "harvest_type": "OACFetcher",
    "write_page": 0,
    "oac": {
      'url': 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb8779p2cx&publisher=%22bancroft%22'
    }
  }
]

oac_datel_harvests = [{
  'collection_id': f"{harvest['collection_id']}-datel",
  'harvest_type': 'DatelOACFetcher',
  'write_page': 0,
  'oac': harvest['oac']
} for harvest in oac_harvests]

json_endpoint_url = "http://dsc-dsc2-dev.cdlib.org/search?facet=type-tab&style=cui&rmode=json&relation="
oac_json_harvests = [{
  "collection_id": f"{harvest['collection_id']}-json",
  "harvest_type": "JsonOAC",
  "write_page": 0,
  "oac": {
    "url": json_endpoint_url + harvest['oac']['url'].split('&relation=')[1]
  }
} for harvest in oac_harvests]