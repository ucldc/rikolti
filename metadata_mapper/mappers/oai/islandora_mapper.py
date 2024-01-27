import requests
from requests.adapters import HTTPAdapter, Retry

from .oai_mapper import OaiRecord, OaiVernacular


class IslandoraRecord(OaiRecord):
    # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/islandora_oai_dc_mapper.py
    # https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py

    def map_subject(self):
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127
        value = self.source_metadata.get('subject')
        if value:
            if isinstance(value, str):
                return [{'name': value}]
            else:
                return [{'name': v} for v in value if v]
        else:
            return None

    def map_is_shown_at(self):
        oai_url = self.source_metadata.get('request_url', '')
        request_url = oai_url.replace('/oai2', '')

        identifier = self.source_metadata.get('id', '')
        _, record_id = identifier.rsplit(':', 1)
        new_id = record_id.replace('_', '%3A', 1)

        if request_url and new_id:
            return f"{request_url}/islandora/object/{new_id}"
        else:
            return None

    def map_is_shown_by(self):
        oai_url = self.source_metadata.get('request_url', '')
        request_url = oai_url.replace('/oai2', '')

        identifier = self.source_metadata.get('id', '')
        _, record_id = identifier.rsplit(':', 1)
        new_id = record_id.replace('_', '%3A', 1)

        if not (request_url and new_id):
            return None

        thumb_url = (
            f"{request_url}/islandora/object/{new_id}/datastream/TN/view")

        # Change URL from 'TN' to 'JPG' for larger versions of image
        # objects & test to make sure the link resolves
        if 'image' or 'StillImage' in self.source_metadata.get('type', ''):
            jpg_url = thumb_url.replace("/TN/", "/JPG/")
            # TODO: should figure out a way to punt a request
            # to minimize the mapper's reliance on external systems
            http = requests.Session()
            retry_strategy = Retry(
                total=3,
                status_forcelist=[413, 429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            request = http.get(jpg_url)
            if request.status_code == 200:
                thumb_url = jpg_url

        return thumb_url


class IslandoraVernacular(OaiVernacular):
    record_cls = IslandoraRecord
