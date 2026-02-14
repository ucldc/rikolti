import json
import logging

from .. import settings
from .Fetcher import Fetcher

class YoutubeFetcher(Fetcher):

    def __init__(self, params):
        super(YoutubeFetcher, self).__init__(params)

        self.harvest_data = params.get("harvest_data", {})
        self.url = self.harvest_data.get("url")
        self.next_page_token = params.get("next_page_token")

    def build_fetch_request(self):
        """
        We expect to receive a url like these, with the playlistid or id parameter supplied:
        - https://www.googleapis.com/youtube/v3/playlistItems?playlistId={id}
        - https://www.googleapis.com/youtube/v3/videos?id={id}
        """

        url = (
            f"{self.url.rstrip('/')}"
            f"&key={settings.YOUTUBE_API_KEY}"
            f"&part=snippet"
            f"&maxResults=50"
        )

        if self.next_page_token:
            url += f"&pageToken={self.next_page_token}"

        return {"url": url}

    def check_page(self, http_resp) -> int:
        data = json.loads(http_resp.content)
        items = data.get("items")

        if len(items) > 0:
            logging.debug(
                f"{self.collection_id}, fetched page {self.write_page} - "
                f"{len(items)} hits,-,-,-,-,-"
            )

        return len(items)

    def increment(self, http_resp):
        self.write_page = self.write_page + 1

        data = http_resp.json()
        self.next_page_token = data.get("nextPageToken")

    def json(self) -> str:
        if not self.next_page_token:
            return json.dumps({"finished": True})
        else:
            return json.dumps({
                "harvest_type": self.harvest_type,
                "collection_id": self.collection_id,
                "harvest_data": self.harvest_data,
                "write_page": self.write_page,
                "next_page_token": self.next_page_token
            })
