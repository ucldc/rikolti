from ..contentdm_mapper import ContentdmRecord, ContentdmVernacular

class ArckRecord(ContentdmRecord):
    @staticmethod
    def identifier_match():
        return ["view/ARCK3D"]

    def UCLDC_map(self):
        return {
            "source": self.source_metadata.get("source")
        }

    def map_is_shown_by(self):
        values = self.source_metadata.get("relation")
        if not values:
            return

        candidates = [v for v in values if "/thumbnail" in v]
        if not candidates:
            return

        return candidates[-1]

    def map_relation(self):
        if self.map_is_shown_at():
            return

        return self.source_metadata.get("relation")


class ArckVernacular(ContentdmVernacular):
    record_cls = ArckRecord
