import requests

from typing import Union

from ..abstract_mapper import AbstractRecord
from .oai_vernacular import OaiVernacular


class CcaVaultRecord(AbstractRecord):

    def to_UCLDC(self) -> 'CcaVaultRecord':
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

    def map_subject(self) -> Union[list[dict[str, str]], None]:
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127
        value = self.source_metadata.get('subject')
        if not value:
            return None

        if isinstance(value, str):
            return [{'name': value}]
        else:
            return [{'name': v} for v in value if v]

    def map_is_shown_at(self) -> Union[str, None]:
        if self.source_metadata.get('identifier'):
            coll_url = (
                self.source_metadata
                .get('identifier')
            )

        ident = self.source_metadata.get('id', '')
        if ":" not in ident:
            return None

        collID, recID = ident.rsplit(':', 1)
        newID = recID.replace('_', '%3A')

        return f"{coll_url}/islandora/object/{newID}"

    def map_is_shown_by(self) -> Union[str, None]:
        if self.source_metadata.get('request_url'):
            coll_url = (
                self.source_metadata
                .get('request_url')
                .replace('/oai2', '')
            )

        ident = self.source_metadata.get('id', '')
        if ':' not in ident:
            return None

        collID, recID = ident.rsplit(':', 1)
        newID = recID.replace('_', '%3A')

        thumb_url = f"{coll_url}/islandora/object/{newID}/datastream/TN/view"

        # Change URL from 'TN' to 'JPG' for larger versions of image
        # objects & test to make sure the link resolves
        if 'image' or 'StillImage' in self.source_metadata.get('type', ''):
            jpg_url = thumb_url.replace("/TN/", "/JPG/")
            request = requests.get(jpg_url)
            if request.status_code == 200:
                thumb_url = jpg_url

        return thumb_url

    def to_dict(self) -> dict[str, str]:
        return self.mapped_data


class CcaVaultVernacular(OaiVernacular):
    record_cls = CcaVaultRecord
