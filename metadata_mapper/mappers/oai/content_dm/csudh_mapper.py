from .contentdm_mapper import ContentdmRecord, ContentdmVernacular


class CsudhRecord(ContentdmRecord):
    def UCLDC_map(self):
        return {
            "identifier": self.map_identifier
        }

    def map_identifier(self):
        identifier = self.collate_fields(['bibliographicCitation', 'identifier'])()

        value = self.source_metadata.get("title", [])

        return identifier + [t for t in value if t.startswith('csudh')]

    def map_title(self):
        if "title" not in self.source_metadata:
            return

        value = self.source_metadata.get("title")

        if isinstance(value, (str, bytes)):
            return value

        return [t for t in value
                if not t.startswith('ark:') and not t.startswith('csudh')]


class CsudhVernacular(ContentdmVernacular):
    record_cls = CsudhRecord
