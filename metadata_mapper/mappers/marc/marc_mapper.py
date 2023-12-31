from ..oai.oai_mapper import OaiVernacular
from ..mapper import Record

from lxml import etree
from sickle import models

from pprint import pprint

class MarcRecord(Record):
    def UCLDC_map(self):
       # pprint(self.source_metadata)
        return {

        }


class MarcVernacular(OaiVernacular):
    pass
