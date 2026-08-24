# ruff: noqa: LOG015, TRY002

import json
import logging
from urllib.parse import urlparse

import requests

from .. import settings
from .Fetcher import Fetcher, FetchError


def is_playlist_url(url):
    return urlparse(url).path == '/youtube/v3/playlistItems'

def is_videos_url(url):
    return urlparse(url).path == '/youtube/v3/videos'

class YoutubeFetcher(Fetcher):

    def __init__(self, params):
        super().__init__(params)

        self.harvest_data = params.get("harvest_data", {})
        self.url = self.harvest_data.get("url")
        self.next_page_token = params.get("next_page_token")

    def build_fetch_request(self):
        """
        We expect to receive a url like these, with the playlistid or id parameter supplied:
        - https://www.googleapis.com/youtube/v3/playlistItems?playlistId={id}
        - https://www.googleapis.com/youtube/v3/videos?id={id}
        """
        if is_playlist_url(self.url):
            # Get the equivalent `videos` endpoint url for current page
            # of playlist items. The `playlistItems` endpoint does not
            # return full metadata.
            videos_url = self.get_videos_url_for_playlist_page()
        elif is_videos_url(self.url):
            videos_url = self.url.strip("/")
        else:
            raise Exception(f"URL {self.url} is not valid."
                            f"Path must be /youtube/v3/playlistItems or /youtube/v3/videos")

        url = (
            f"{videos_url}"
            f"&key={settings.YOUTUBE_API_KEY}"
            f"&part=snippet"
            f"&maxResults=50"
        )

        if not is_playlist_url(self.url) and self.next_page_token:
            url += f"&pageToken={self.next_page_token}"

        request = {"url": url}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def get_videos_url_for_playlist_page(self):
        # Hit the `playlistItems` endpoint to get a page of items
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

        # Construct a `videos` endpoint url for the page of playlist items,
        # where the `id` parameter is a comma-separated list of 50 video_ids
        video_ids = [item.get("contentDetails").get("videoId") for item in json_response.get("items",[])]
        videos_url = f"https://www.googleapis.com/youtube/v3/videos?id={','.join(video_ids)}"

        return videos_url

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

        if not is_playlist_url(self.url):
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
