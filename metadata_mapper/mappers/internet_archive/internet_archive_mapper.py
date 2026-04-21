import json

from ..mapper import Record, Vernacular

class InternetArchiveRecord(Record):
    def UCLDC_map(self) -> dict:
        return {
            "calisphere-id": self.source_metadata.get("identifier"),
            "isShownAt": self.map_is_shown_at(),
            "isShownBy": self.map_is_shown_by(),
            "relation": self.source_metadata.get("collection"),
            "date": self.source_metadata.get("date"),
            "description": self.source_metadata.get("description"),
            "format": self.source_metadata.get("format"),
            "identifier": self.string_to_list(self.source_metadata.get("identifier")),
            "language": self.source_metadata.get("language"),
            "type": self.source_metadata.get("mediatype"),
            "rights": self.source_metadata.get("rights"),
            "publisher": self.source_metadata.get("publisher"),
            "subject": self.map_subject(),
            "title": self.string_to_list(self.source_metadata.get("title")),
            "contributor": self.string_to_list(self.source_metadata.get("contributor")),
            "creator": self.string_to_list(self.source_metadata.get("creator"))
        }
    
    def map_is_shown_at(self):
        identifier = self.source_metadata['identifier']

        return f"https://archive.org/details/{identifier}"

    def map_is_shown_by(self):
        identifier = self.source_metadata['identifier']

        return f"https://archive.org/services/img/{identifier}"

    def map_subject(self) -> list:
        subjects = self.source_metadata.get("subject", [])
        if isinstance(subjects, str):
            subjects = [subjects]

        return [{"name": subject} for subject in subjects]

    def string_to_list(self, value) -> list:
        if isinstance(value, str):
            value = [value]

        return value

class InternetArchiveVernacular(Vernacular):
    record_cls = InternetArchiveRecord

    def parse(self, api_response):
        page_element = json.loads(api_response)
        records = page_element.get("response", {}).get("docs", [])
        return self.get_records([record for record in records])