from .oai_mapper import OaiRecord, OaiVernacular


class RecollectRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.source_metadata.get("relation"),
            "relation": None
        }

    def map_is_shown_at(self):
        for identifier in self.source_metadata.get("identifier", []):
            if identifier.startswith("http"):
                return identifier

class RecollectVernacular(OaiVernacular):
    record_cls = RecollectRecord
