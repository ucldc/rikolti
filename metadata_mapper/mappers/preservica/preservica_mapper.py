import json

from ..mapper import Vernacular, Record

class PreservicaRecord(Record):
    def to_UCLDC(self):
        id_handle = self.source_metadata.get('id')
        self.legacy_couch_db_id = f"{self.collection_id}--{id_handle}"
        return super().to_UCLDC()

    def UCLDC_map(self) -> dict[str]:
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "contributor": self.ensure_list(self.source_metadata.get("oai_dc.contributor")),
            "spatial": self.ensure_list(self.source_metadata.get("oai_dc.coverage")),
            "creator": self.ensure_list(self.source_metadata.get("oai_dc.creator")),
            "date": self.source_metadata.get("oai_dc.date"),
            "description": self.ensure_list(self.source_metadata.get("oai_dc.description")),
            "format": self.ensure_list(self.source_metadata.get("oai_dc.format")),
            "identifier": self.ensure_list(self.source_metadata.get("oai_dc.identifier")),
            "language": self.ensure_list(self.source_metadata.get("oai_dc.language")),
            "publisher": self.ensure_list(self.source_metadata.get("oai_dc.publisher")),
            "relation": self.ensure_list(self.source_metadata.get("oai_dc.relation")),
            "rights": self.ensure_list(self.source_metadata.get("oai_dc.rights")),
            "source": self.ensure_list(self.source_metadata.get("oai_dc.source")),
            # "subject": self.ensure_list(self.source_metadata.get("oai_dc.subject")),
            "subject": self.map_subject,
            "title": self.ensure_list(self.source_metadata.get("oai_dc.title")),
            "type": self.ensure_list(self.source_metadata.get("oai_dc.type")),
        }
    
    def map_is_shown_at(self) -> str:
        id = self.source_metadata.get('id')

        return f"https://oakland.access.preservica.com/file/sdb:digitalFile%7C{id}/"
    
    def map_is_shown_by(self) -> str:
        id = self.source_metadata.get('id')
        return f"https://oakland.access.preservica.com/download/thumbnail/sdb:digitalFile%7C{id}"

    def map_subject(self) -> list:
        subjects = self.source_metadata.get("oai_dc.subject")
        if subjects:
            subjects = self.ensure_list(subjects)
            subject = [{"name": subject} for subject in subjects]

            return subject

class PreservicaVernacular(Vernacular):
    record_cls = PreservicaRecord

    def parse(self, api_response):
        data = json.loads(api_response)
        vernacular_metadata = data.get("value",{}).get("metadata", [])
        records = []
        for vm in vernacular_metadata:
            record = {}
            for field in vm:
                record[field['name']] = field['value']
            records.append(record)

        return self.get_records(records)
