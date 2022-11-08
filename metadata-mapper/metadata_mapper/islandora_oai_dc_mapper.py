import os
import json
from mapper import VernacularReader, Record
from lxml import etree
from sickle import models
import requests


# https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/islandora_oai_dc_mapper.py
# https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
class OAIVernacular(VernacularReader):

    def parse(self, api_response):
        namespace = {'oai2': 'http://www.openarchives.org/OAI/2.0/'}
        page = etree.XML(api_response)

        request_elem = page.find('oai2:request', namespace)
        if request_elem is not None:
            request_url = request_elem.text
        else:
            request_url = None

        record_elements = (
            page
            .find('oai2:ListRecords', namespace)
            .findall('oai2:record', namespace)
        )

        records = []
        for re in record_elements:
            sickle_rec = models.Record(re)
            sickle_header = sickle_rec.header
            if not sickle_header.deleted:
                record = sickle_rec.metadata
                record['datestamp'] = sickle_header.datestamp
                record['id'] = sickle_header.identifier
                record['request_url'] = request_url
                records.append(record)

        records = [self.record_cls(self.collection_id, rec) for rec in records]
        return records

    # lxml parser requires bytes input or XML fragments without declaration,
    # so use 'rb' mode
    def get_local_api_response(self):
        local_path = self.local_path('vernacular_metadata')
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "rb")
        api_response = page.read()
        return api_response


class IslandoraRecord(Record):

    def to_UCLDC(self):
        # print(f"{self.source_metadata=}")

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


class IslandoraVernacular(OAIVernacular):
    record_cls = IslandoraRecord
