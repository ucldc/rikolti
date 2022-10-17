import os
import json
from mapper import VernacularReader, Record
from xml.etree import ElementTree
from lxml import etree
from sickle import models


NS = {'oai2': 'http://www.openarchives.org/OAI/2.0/'}

# Islandora_OAIMapper(DublinCoreMapper)
# https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/islandora_oai_dc_mapper.py
# https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
class IslandoraVernacular(VernacularReader):

    def __init__(self, payload):
            super(IslandoraVernacular, self).__init__(payload)
            self.record_cls = IslandoraRecord

    def parse(self, api_response):
        print(f"{type(api_response)=}")
        page = etree.XML(api_response)

        record_elements = page.find('oai2:ListRecords', NS).findall('oai2:record', NS)

        records = []
        for re in record_elements:
            sickle_rec = models.Record(re)
            sickle_header = sickle_rec.header
            if not sickle_header.deleted:
                record = sickle_rec.metadata
                record['datestamp'] = sickle_header.datestamp
                record['id'] = sickle_header.identifier
                records.append(record)

        records = [self.record_cls(self.collection_id, rec) for rec in records]
        return records

    # lxml parser requires bytes input or XML fragments without declaration, so use 'rb' mode
    def get_local_api_response(self):
        local_path = self.local_path('vernacular_metadata')
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "rb")
        api_response = page.read()
        return api_response

    '''
    def get_s3_api_response(self):
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"vernacular_metadata/{self.collection_id}/{self.page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        api_response = s3_obj_summary['Body'].read()
        return api_response
    '''

class IslandoraRecord(Record):

    def to_UCLDC(self):
        self.mapped_data = {}

        return self

    def to_dict(self):
        return self.mapped_data
