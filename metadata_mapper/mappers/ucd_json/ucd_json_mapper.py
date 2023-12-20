import json

from ..mapper import Vernacular, Record


class UcdJsonRecord(Record):

    BASE_URL = "https://digital.ucdavis.edu"

    def UCLDC_map(self) -> dict[str]:
        self.legacy_couch_db_id = self.get_legacy_couch_id()

        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.BASE_URL + self.source_metadata.get("@id"),
            "isShownBy": self.BASE_URL + self.source_metadata.get("thumbnailUrl"),
            "title": self.map_title,
            "date": self.source_metadata.get("datePublished"),
            "description": self.map_description,
            "subject": self.map_subject,
            "format": self.source_metadata.get("material", []),
            "creator": self.map_creator,
            "identifier": self.source_metadata.get("identifier"),
            "publisher": self.map_publisher,
            "type": self.source_metadata.get("type"),
            "rightsURI": self.source_metadata.get("license")
        }

    def get_legacy_couch_id(self) -> str:
        ark = [v for v in self.source_metadata.get("identifier", [])
               if v.startswith("ark:")]

        return f"{self.collection_id}--{ark[0]}"

    def map_title(self) -> list:
        value = self.source_metadata.get("name", [])

        if isinstance(value, list):
            return value

        return [value]

    def map_description(self) -> list:
        value = self.source_metadata.get("description", [])

        if isinstance(value, list):
            return value

        return [value]

    def map_subject(self) -> list:
        value = self.source_metadata.get("about", [])
        # Wrap dicts in lists, see collection 8, item ark:/13030/tf629006kp
        if isinstance(value, dict):
            value = [value]

        return [{"name": v.get("name")} for v in value if "name" in v]

    def map_creator(self) -> list:
        value = self.source_metadata.get("creator", [])

        # Wrap dicts in lists, see collection 8, item ark:/13030/tf4c6004gh
        if isinstance(value, dict):
            value = [value]

        return [v.get("name") for v in value if "name" in v]

    def map_publisher(self) -> list:
        value = self.source_metadata.get("publisher", [])

        if isinstance(value, dict):
            value = [value]

        return [v.get("name") for v in value if "name" in v]


class UcdJsonVernacular(Vernacular):
    record_cls = UcdJsonRecord

    def skip(self, record: UcdJsonRecord) -> bool:
        """
        If we are missing a thumbnailUrl or an ark, things will go wrong in the mapping
        """
        if not record.get("thumbnailUrl"):
            return True

        return not any([v.startswith("ark:") for v
                        in record.get("identifier", [])])

    def parse(self, api_response: str) -> list:
        records = []
        for record in json.loads(api_response):
            record.update({"metadata/identifier": "@id"})
            records.append(record)

        return self.get_records(records)
