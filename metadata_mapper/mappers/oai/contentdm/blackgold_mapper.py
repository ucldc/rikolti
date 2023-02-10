from .contentdm_mapper import ContentdmRecord, ContentdmVernacular


class BlackgoldRecord(ContentdmRecord):
    """Appears to map ok"""
    @staticmethod
    def identifier_match():
        return ["luna/servlet/detail"]

    def map_is_shown_by(self):
        values = self.source_metadata.get("identifier")
        if not values:
            return

        candidates = [v for v in values if "mediafile=" in v]
        if not candidates:
            return

        return candidates[-1].replace("Size2", "Size4")


class BlackgoldVernacular(ContentdmVernacular):
    record_cls = BlackgoldRecord
