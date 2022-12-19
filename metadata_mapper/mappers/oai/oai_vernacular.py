import os
import settings

from lxml import etree
from sickle import models

from ..mapper import Vernacular


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

        records = [self.record_cls(self.collection_id, rec) for rec in records]
        return records

    # lxml parser requires bytes input or XML fragments without declaration,
    # so use 'rb' mode
    def get_local_api_response(self):
        local_path = settings.local_path(
            'vernacular_metadata', self.collection_id)
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "rb")
        api_response = page.read()
        return api_response
