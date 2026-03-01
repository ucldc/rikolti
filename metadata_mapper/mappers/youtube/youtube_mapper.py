import json

from ..mapper import Record, Vernacular

class YoutubeRecord(Record):
    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split('--')[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "description": self.map_description,
            "subject": self.map_subject,
            "title": self.map_title
        }

    def map_is_shown_at(self):
        if self.source_metadata.get('id'):
            return f"https://www.youtube.com/watch?v={self.source_metadata.get('id')}"
    
    def map_is_shown_by(self):
        thumbnails = self.source_metadata.get("snippet",{}).get("thumbnails",{})
        return thumbnails.get("standard", {}).get("url")

    def map_description(self):
        return self.source_metadata.get("snippet", {}).get("description")

    def map_subject(self):
        tags = self.source_metadata.get("snippet", {}).get("tags")

        return [{"name": tag} for tag in tags]

    def map_title(self):
        return self.source_metadata.get("snippet", {}).get("title")


class YoutubeVernacular(Vernacular):
    record_cls = YoutubeRecord

    def parse(self, api_response):
        def modify_record(record):
            record.update({"calisphere-id": f"{self.collection_id}--"
                                            f"{record.get('id')}"})
            return record

        records = json.loads(api_response).get("items")
        records = [modify_record(record) for record in json.loads(api_response).get("items")]
        return self.get_records(records)
