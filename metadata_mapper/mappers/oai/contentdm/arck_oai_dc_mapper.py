from ..contentdm_oai_dc_mapper import ContentdmOaiDcRecord, ContentdmOaiDcVernacular

class ArckOaiDcRecord(ContentdmOaiDcRecord):
    @staticmethod
    def __identifier_match():
        return "view/ARCK3D"

    def map_is_shown_by(self):
        return self.get_last_match(self.source_metadata.get("relation", "/thumbnail"))

    def map_source(self):
        return self.source_metadata.get("source")

    def map_relation(self):
        if self.map_is_shown_at():
            return

        return self.source_metadata.get("relation")

class ContentdmOaiDcVernacular(ContentdmOaiDcVernacular):
    record_cls = ContentdmOaiDcRecord
