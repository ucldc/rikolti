import json

from ..mapper import Vernacular, Record

class PreservicaRecord(Record):
    def to_UCLDC(self):
        id_handle = self.remove_id_prefix(self.source_metadata.get('id'))
        self.legacy_couch_db_id = f"{self.collection_id}--{id_handle}"
        return super().to_UCLDC()

    def UCLDC_map(self) -> dict[str]:
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "contributor": self.source_metadata.get("contributor"),
            "spatial": self.source_metadata.get("coverage"),
            "creator": self.source_metadata.get("creator"),
            "date": self.source_metadata.get("date"),
            "description": self.source_metadata.get("description"),
            "format": self.source_metadata.get("format"),
            "identifier": self.source_metadata.get("identifier"),
            "language": self.source_metadata.get("language"),
            "publisher": self.source_metadata.get("publisher"),
            "relation": self.source_metadata.get("relation"),
            "rights": self.source_metadata.get("rights"),
            "source": self.source_metadata.get("source"),
            "subject": self.map_subject,
            "title": self.source_metadata.get("title"),
            "type": self.source_metadata.get("type"),
            "stateLocatedIn": [{"name": "California"}]
        }
    
    def remove_id_prefix(self, id) -> str:
        return id.removeprefix('sdb:IO|')

    def map_is_shown_at(self) -> str:
        id = self.remove_id_prefix(self.source_metadata.get('id'))
        return f"https://oakland.access.preservica.com/file/sdb:digitalFile%7C{id}/"
    
    def map_is_shown_by(self) -> str:
        id = self.remove_id_prefix(self.source_metadata.get('id'))
        return f"https://oakland.access.preservica.com/download/thumbnail/sdb:digitalFile%7C{id}"

    def map_subject(self) -> list:
        subjects = self.source_metadata.get("subject")
        if subjects:
            return [{"name": subject} for subject in subjects]

class PreservicaVernacular(Vernacular):
    record_cls = PreservicaRecord

    def parse(self, api_response):
        data = json.loads(api_response)
        records = []
        for item in data:
            record = {}
            record["id"] = item.get("id")

            dc_metadata = [
                    group for group
                    in item.get("metadata", {}).get("groupOrItem", [])
                    if group.get("title") == "Dublin Core Metadata"
                ]

            for field in dc_metadata[0]["groupOrItem"]:
                field_name = field["name"]
                field_value = field["value"]
                if field_value:
                    if field_name in record:
                        record[field_name].append(field_value)
                    else:
                        record[field_name] = [field_value]

            records.append(record)

        return self.get_records(records)
