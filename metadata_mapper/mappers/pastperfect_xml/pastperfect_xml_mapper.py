from xml.etree import ElementTree

from ..mapper import Vernacular, Record

class PastperfectXmlRecord(Record):

    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "title": self.source_metadata.get("title"),
            "date": self.source_metadata.get("date"),
            "description": self.source_metadata.get("title"),
            "subject": self.map_subject,
            "spatial": self.source_metadata.get("place"),
            "temporal": self.source_metadata.get("coverage"),
            "format": self.collate_fields(["medium", "material"]),
            "creator": self.collate_fields(["creator", "author", "artist",
                                            "photographer"]),
            "identifier": self.collate_fields(["identifier", "objectid", "arkid"]),
            "type": self.source_metadata.get("objectname"),
            "relation": self.source_metadata.get("collection"),
            "rights": self.source_metadata.get("rights")
        }

    def map_is_shown_at(self):
        return self.source_metadata.get("url", None)

    def map_is_shown_by(self):
        thumbnail = self.source_metadata.get("thumbnail")
        if ".tif" in thumbnail:
            return thumbnail.replace(".tif", ".jpg")
        return thumbnail

    def map_subject(self):
        values = self.collate_fields(["subject", "people", "searchterms"])()
        return [{"name": value} for value in values]


class PastperfectXmlVernacular(Vernacular):
    record_cls = PastperfectXmlRecord

    def skip(self, record):
        return not record.get("thumbnail", False)

    def parse(self, api_response):
        xml = ElementTree.fromstring(api_response)
        data_nodes = xml.findall('.//record/metadata/PPO-Data')

        records = []
        for node in data_nodes:
            record = {}
            for data_element in node:
                record[data_element.tag] = data_element.text
            records.append(record)

        return self.get_records(records)
