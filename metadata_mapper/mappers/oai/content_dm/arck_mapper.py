from .contentdm_mapper import ContentdmRecord, ContentdmVernacular


class ArckRecord(ContentdmRecord):
    identifier_match = "view/ARCK3D"

    def UCLDC_map(self):
        return {
            "source": self.source_metadata.get("source"),
            "isShownBy": self.source_metadata.get("relation"),
            "relation": None
        }


class ArckVernacular(ContentdmVernacular):
    record_cls = ArckRecord
