from .oai_mapper import OaiRecord, OaiVernacular


class SamveraRecord(OaiRecord):

    def map_is_shown_at(self) -> str:
        value = self.source_metadata.get("isShownAt")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None

    def map_is_shown_by(self) -> str:
        value = self.source_metadata.get("object")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None


class SamveraVernacular(OaiVernacular):
    record_cls = SamveraRecord
