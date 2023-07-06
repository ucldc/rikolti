import json
from .Fetcher import Fetcher
import requests
from requests.adapters import HTTPAdapter
from requests.adapters import Retry
from urllib.parse import urlencode
import settings


class YoutubeFetcher(Fetcher):
    BASE_URL = "https://www.googleapis.com/youtube/v3/"

    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(YoutubeFetcher, self).__init__(params)

        # If `next_url` is a param, we know that this is not the fetch of the
        # first page, so skip setting those attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        self.collection_id = params.get("collection_id")
        self.external_id = params.get("harvest_data").get("harvest_extra_data")
        # 50 is the max per_page value
        self.per_page = 1
        self.page_token = None
        self.next_page_token = None
        self.next_url = self.get_current_url()

    def get_current_url(self) -> str:
        """
        If it's a single video, fetch it, otherwise fetch a page of results from a
        playlist or user upload.

        Returns: str
        """
        if self.is_single():
            return self.get_videos_by_id_request([self.external_id])

        params = {
            "maxResults": self.per_page,
            "part": "contentDetails",
            "playlistId": self.external_id,
            "pageToken": self.page_token or ""
        }
        endpoint = "playlistItems"

        return self.build_request_url(endpoint, params)

    def is_single(self) -> bool:
        """
        Based on the external_id, determines if it's a single video in the
        simplest way possible.

        Returns: bool
        """
        return len(self.external_id) == 11

    def get_videos_by_id_request(self, external_ids: list) -> str:
        """
        Parameters:
            external_ids: list

        Returns: str
        """
        params = {
            "part": "snippet",
            "id": ",".join(external_ids)
        }
        endpoint = "videos"

        return self.build_request_url(endpoint, params)

    def build_request_url(self, endpoint: str, params: dict[str]) -> str:
        """
        Creates a YouTube API request URL from dictionary of request parameters.

        Parameters:
            endpoint: str
            params: dict[str]

        Returns: str
        """
        params["key"] = settings.YOUTUBE_API_KEY

        return self.BASE_URL + endpoint + "?" + urlencode(params)

    def build_fetch_request(self) -> dict[str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        request = {"url": self.get_current_url()}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def aggregate_vernacular_content(self, response: requests.Response) -> str:
        """
        If it's a single video, this response is our page content. Otherwise, we
        need to iterate the returned records and fetch them by id.

        Parameters:
            response: requests.Response

        Returns: str
        """
        content = response.text
        if self.is_single():
            return content

        items = json.loads(content)

        external_ids = [item.get("contentDetails").get("videoId")
                        for item in items.get("items")]

        return self.get_video_metadata(external_ids).text

    def get_video_metadata(self, external_ids: list) -> requests.Response:
        """
        Performs a request for video info and returns the response. Attempts
        retries.

        Parameters:
            external_ids: list

        Returns: requests.Response
        """
        print(
            f"[{self.collection_id}]: Fetching videos {external_ids}"
        )

        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session.get(url=self.get_videos_by_id_request(external_ids))

    def check_page(self, http_resp: requests.Response) -> int:
        """
        Parameters:
            http_resp: requests.Response

        Returns: int
        """
        data = json.loads(http_resp.content)
        count = len(data.get("items"))

        print(
            f"[{self.collection_id}]: Fetched ids for page {self.write_page} "
            f"at {http_resp.url} with {count} hits"
        )

        return count

    def increment(self, http_resp: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
             http_resp: requests.Response
        """
        super(YoutubeFetcher, self).increment(http_resp)

        data = json.loads(http_resp.content)
        self.next_page_token = data.get("nextPageToken", None)

        self.next_url = self.get_current_url() if self.next_page_token else None

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "external_id": self.external_id,
            "collection_id": self.collection_id,
            "next_url": self.next_url,
            "page_token": self.next_page_token,
            "write_page": self.write_page,
            "per_page": self.per_page
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
