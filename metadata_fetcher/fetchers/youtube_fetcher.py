import json
import logging
import requests
from urllib.parse import urlparse

from .. import settings
from .Fetcher import Fetcher, FetchError

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
        if urlparse(self.url).path == '/youtube/v3/playlistItems':
            # The `playlistItems` API endpoint returns incomplete metadata, so we hit it
            # for a page of video ids, and then hit the `videos` endpoint for full metadata.
            playlist_content_url = (
                f"{self.url}"
                f"&key={settings.YOUTUBE_API_KEY}"
                f"&part=contentDetails"
                f"&maxResults=50"
            )

            if self.next_page_token:
                playlist_content_url += f"&pageToken={self.next_page_token}"

            playlist_content_request = {"url": playlist_content_url}
            try:
                response = self.http_session.get(**playlist_content_request)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise FetchError(
                    f"[{self.collection_id}]: unable to fetch {playlist_content_request}",
                    f"Error was: {e}"
                )

            json_response = response.json()

            self.next_page_token = json_response.get("nextPageToken")

            video_ids = [item.get("contentDetails").get("videoId") for item in json_response.get("items",[])]
            videos_url = f"https://www.googleapis.com/youtube/v3/videos?id={','.join(video_ids)}"
        else:
            videos_url = self.url

        url = (
            f"{videos_url}"
            f"&key={settings.YOUTUBE_API_KEY}"
            f"&part=snippet"
            f"&maxResults=50"
        )

        request = {"url": url}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def check_page(self, http_resp) -> int:
        data = json.loads(http_resp.content)
        items = data.get("items")

        if len(items) > 0:
            logging.debug(
                f"{self.collection_id}, fetched page {self.write_page} - "
                f"{len(items)} hits,-,-,-,-,-"
            )

        return len(items)

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
