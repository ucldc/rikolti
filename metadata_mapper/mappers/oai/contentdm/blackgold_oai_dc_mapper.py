from typing import Union

from ..contentdm_oai_dc_mapper import ContentdmOaiDcRecord, ContentdmOaiDcVernacular

class ArckOaiDcRecord(ContentdmOaiDcRecord):
    @staticmethod
    def __identifier_match():
        return "luna/servlet/detail"

    def map_is_shown_by(self):
        value = self.get_last_match(self.source_metadata.get("identifier", "mediafile="))
        if not value:
            return

        return value.replace("Size2", "Size4")


class ContentdmOaiDcVernacular(ContentdmOaiDcVernacular):
    record_cls = ContentdmOaiDcRecord
