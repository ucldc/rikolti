import requests

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
        if self.source_metadata.get('request_url'):
            coll_url = (
                self.source_metadata
                .get('request_url')
                .replace('/oai2', '')
            )

        ident = self.source_metadata.get('id', '')
        if ':' in ident:
            collID, recID = ident.rsplit(':', 1)
            newID = recID.replace('_', '%3A')

            return f"{coll_url}/islandora/object/{newID}"
        else:
            return None

    def map_is_shown_by(self):
        if self.source_metadata.get('request_url'):
            coll_url = (
                self.source_metadata
                .get('request_url')
                .replace('/oai2', '')
            )

        ident = self.source_metadata.get('id', '')
        if ':' in ident:
            collID, recID = ident.rsplit(':', 1)
            newID = recID.replace('_', '%3A')

            thumb_url = (
                f"{coll_url}/islandora/object/{newID}/datastream/TN/view")

            # Change URL from 'TN' to 'JPG' for larger versions of image
            # objects & test to make sure the link resolves
            if 'image' or 'StillImage' in self.source_metadata.get('type', ''):
                jpg_url = thumb_url.replace("/TN/", "/JPG/")
                # TODO: should figure out a way to punt a request
                # to minimize the mapper's reliance on external systems
                request = requests.get(jpg_url)
                if request.status_code == 200:
                    thumb_url = jpg_url

            return thumb_url
        else:
            return None


class IslandoraVernacular(OaiVernacular):
    record_cls = IslandoraRecord
