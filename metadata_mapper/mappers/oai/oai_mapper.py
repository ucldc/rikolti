import os
import settings

from lxml import etree
from sickle import models

from ..mapper import Record, Vernacular


class OaiRecord(Record):
    """Superclass for OAI metadata."""

    def UCLDC_map(self):
        return {
            'contributor': self.working_metadata.get('contributor'),
            'creator': self.working_metadata.get('creator'),
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
            'extent': self.working_metadata.get('extent'),
            'format': self.collate_fields(["format", "medium"]),
            'identifier': self.collate_fields(
                ["bibliographicCitation", "identifier"]),
            'is_shown_by': self.map_is_shown_by(),
            'is_shown_at': self.map_is_shown_at(),
            'provenance': self.working_metadata.get('provenance'),
            'publisher': self.working_metadata.get('publisher'),
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
            'subject': self.working_metadata.get("subject"),
            'temporal': self.working_metadata.get('temporal'),
            'title': self.working_metadata.get('title'),
            'type': self.working_metadata.get('type')
        }


class OaiVernacular(Vernacular):

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

        return [self.record_cls(self.collection_id, rec) for rec in records]

    # lxml parser requires bytes input or XML fragments without declaration,
    # so use 'rb' mode
    def get_local_api_response(self):
        local_path = settings.local_path(
            'vernacular_metadata', self.collection_id)
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "rb")
        api_response = page.read()
        return api_response
