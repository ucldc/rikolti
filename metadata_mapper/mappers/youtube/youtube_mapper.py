import json
from typing import Optional
from ..mapper import Vernacular, Record


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

    def map_is_shown_at(self) -> str:
        video_id = self.source_metadata.get("id")
        return f"https://www.youtube.com/watch?v={video_id}"

    def map_is_shown_by(self) -> Optional[str]:
        thumbnails = self.source_metadata.get("snippet", {}).get("thumbnails")
        for thumb_label in ["standard", "high", "medium", "default"]:
            thumb_url = thumbnails.get(thumb_label, {}).get("url")
            if thumb_url is not None:
                break
        else:
            thumb_url = None

        return thumb_url

    def map_description(self) -> list:
        return [self.source_metadata.get("snippet", {}).get("description")]

    def map_subject(self) -> list:
        return [{"name": v} for v in
                self.source_metadata.get("snippet", {}).get("tags", [])]

    def map_title(self) -> list:
        return [self.source_metadata.get("snippet", {}).get("title")]


class YoutubeVernacular(Vernacular):
    record_cls = YoutubeRecord

    def parse(self, api_response: str) -> list:
        def modify_record(record: dict) -> dict:
            record.update({"calisphere-id": f"{self.collection_id}--"
                                            f"{record.get('id')}"})
            return record

        records = [modify_record(record) for record
                   in json.loads(api_response).get("items")]

        records = self.get_records(records)
        return records

