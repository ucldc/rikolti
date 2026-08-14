import re

from ..omeka_mapper import OmekaRecord, OmekaVernacular


class ChsscRecord(OmekaRecord):
    def UCLDC_map(self):
        return {
            "isShownAt": self.map_is_shown_at
        }

    def map_is_shown_at(self):
        for identifier in self.source_metadata.get('identifier'):
            identifier = identifier.strip()
            if re.search(r"^http*s:\/\/", identifier) \
            and re.search(r"/item/[0-9]+/?$", identifier):
                return identifier

class ChsscVernacular(OmekaVernacular):
    record_cls = ChsscRecord
