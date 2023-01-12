import requests

from ..mapper import Record
from .oai_vernacular import OaiVernacular


class IslandoraRecord(Record):
    # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/islandora_oai_dc_mapper.py
    # https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py

    def to_UCLDC(self):
        self.mapped_data = {
            'contributor': self.source_metadata.get('contributor'),
            'creator': self.source_metadata.get('creator'),
            'date': self.collate_fields([
                "available",
                "created",
                "date",
                "dateAccepted",
                "dateCopyrighted",
                "dateSubmitted",
                "issued",
                "modified",
                "valid"
            ]),
            'description': self.collate_fields([
                "abstract",
                "description",
                "tableOfContents"
            ]),
            'extent': self.source_metadata.get('extent'),
            'format': self.collate_fields(["format", "medium"]),
            'identifier': self.collate_fields(
                ["bibliographicCitation", "identifier"]),
            'is_shown_by': self.map_is_shown_by(),
            'is_shown_at': self.map_is_shown_at(),
            'provenance': self.source_metadata.get('provenance'),
            'publisher': self.source_metadata.get('publisher'),
            'relation': self.collate_fields([
                "conformsTo",
                "hasFormat",
                "hasPart",
                "hasVersion",
                "isFormatOf",
                "isPartOf",
                "isReferencedBy",
                "isReplacedBy",
                "isRequiredBy",
                "isVersionOf",
                "references",
                "relation",
                "replaces",
                "requires"
            ]),
            'rights': self.collate_fields(["accessRights", "rights"]),
            'spatial': self.collate_fields(["coverage", "spatial"]),
            'subject': self.map_subject(),
            'temporal': self.source_metadata.get('temporal'),
            'title': self.source_metadata.get('title'),
            'type': self.source_metadata.get('type'),
        }
        return self

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
                request = requests.get(jpg_url)
                if request.status_code == 200:
                    thumb_url = jpg_url

            return thumb_url
        else:
            return None

    def to_dict(self):
        return self.mapped_data


class IslandoraVernacular(OaiVernacular):
    record_cls = IslandoraRecord
