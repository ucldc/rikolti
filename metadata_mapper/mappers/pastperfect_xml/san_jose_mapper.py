from xml.etree import ElementTree

from ..mapper import Record, Vernacular


class SanJoseRecord(Record):
    def to_UCLDC(self):
        id_handle = self.source_metadata["identifier"].strip().replace(" ", "__")
        self.legacy_couch_db_id = f"{self.collection_id}--{id_handle}"
        return super().to_UCLDC()

    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.source_metadata.get("url"),
            "isShownBy": self.source_metadata.get("thumbnail"),
            "title": self.ensure_list(self.source_metadata.get("title")),
            "date": self.source_metadata.get("date"),
            "description": self.ensure_list(self.source_metadata.get("description")),
            "subject": self.map_subject,
            "temporal": self.source_metadata.get("coverage"),
            "creator": self.source_metadata.get("creator"),
            "identifier": self.collate_fields(["identifier", "objectid"]),
            "type": self.source_metadata.get("type"),
            "relation": self.ensure_list(self.source_metadata.get("collection")),
            "rights": self.source_metadata.get("rights")
        }

    def map_subject(self):
        values = self.collate_fields(["subject", "people", "searchterms"])()
        return [{"name": value} for value in values]

class SanJoseVernacular(Vernacular):
    record_cls = SanJoseRecord

    def skip(self, record):
        return not record.get("thumbnail", False)

    def parse(self, api_response):
        xml = ElementTree.fromstring(api_response)
        data_nodes = xml.findall('.//record/metadata/PPWE-Data')
        records = []
        for node in data_nodes:
            record = {}
            for data_element in node:
                record[data_element.tag] = data_element.text
            records.append(record)

        return self.get_records(records)
