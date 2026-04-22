import json

from ..mapper import Record, Vernacular

class YoutubeRecord(Record):
    def UCLDC_map(self):
        self.legacy_couch_db_id = f"{self.collection_id}--{self.get_video_id()}"

        return {
            "calisphere-id": self.get_video_id,
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "description": self.map_description,
            "subject": self.map_subject,
            "title": self.map_title
        }

    def get_video_id(self):
        if self.source_metadata.get("kind") == "youtube#playlistItem":
            return self.source_metadata.get("snippet", {}).get("resourceId", {}).get("videoId")
        elif self.source_metadata.get("kind") == "youtube#video":
            return self.source_metadata.get("id")

    def map_is_shown_at(self):
        video_id = self.get_video_id()

        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    def map_is_shown_by(self):
        thumbnails = self.source_metadata.get("snippet",{}).get("thumbnails",{})

        return thumbnails.get("standard", {}).get("url")

    def map_description(self):
        description = self.source_metadata.get("snippet", {}).get("description")

        return self.string_to_list(description)

    def map_subject(self):
        tags = self.source_metadata.get("snippet", {}).get("tags", [])

        return [{"name": tag} for tag in tags]

    def map_title(self):
        title = self.source_metadata.get("snippet", {}).get("title")

        return self.string_to_list(title)

    def string_to_list(self, value) -> list:
        if isinstance(value, str):
            value = [value]

        return value


class YoutubeVernacular(Vernacular):
    record_cls = YoutubeRecord

    def parse(self, api_response):
        records = json.loads(api_response).get("items")

        return self.get_records(records)
