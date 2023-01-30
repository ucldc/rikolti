from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular

class UcscOaiDcRecord(OaiRecord):
    def map_is_shown_at(self):
        if "isShownAt" not in self.source_metadata:
            return

        return self.source_metadata.get("isShownAt")

    def map_is_shown_by(self):
        """At time of creation, I'm not seeing any examples of this field existing in vernacular metadata"""

        if "isShownBy" not in self.source_metadata:
            return

        return self.source_metadata.get("isShownBy")

class UcscOaiDcVernacular(OaiVernacular):
    record_cls = UcscOaiDcRecord
