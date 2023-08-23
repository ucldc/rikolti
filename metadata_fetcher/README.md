Entry points:

python -m metadata_fetcher.lambda_function '{"collection_id": 26098, "harvest_type": "nuxeo", "harvest_data": {"harvest_extra_data": "/asset-library/UCI/SCA_SpecialCollections/MS-R016/Cochems", "url": "https://nuxeo.cdlib.org/Nuxeo/site/api/v1"}}'

python -m metadata_fetcher.lambda_function '{"collection_id": 26098, "harvest_type": "nuxeo", "harvest_data": {"harvest_extra_data": "/asset-library/UCI/SCA_SpecialCollections/MS-R016/Cochems", "url": "https://nuxeo.cdlib.org/Nuxeo/site/api/v1"}}'

python -m metadata_fetcher.lambda_function '{"collection_id": 27364, "harvest_data": {"harvest_extra_data": "72157709037967781", "url": "http://example.edu"}, "harvest_type": "flickr", "rikolti_mapper_type": "flickr.sppl"}'

python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/?harvest_type=NUX&format=json"

Artist Books (complex image objects)
python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/26147/?format=json"

Anthill (pdf objects)
python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/26695/?format=json"

Viet Stories (complex multi-format objects)
python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/36/?format=json"

Nightingale (complex image)
python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/76/?format=json"

Halpern (simple & complex video objects)
python -m metadata_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/27694/?format=json"

Ramicova (simple image objects)
python -m metadat_fetcher.fetch_registry_collections "https://registry.cdlib.org/api/v1/rikoltifetcher/26098/?format=json"
