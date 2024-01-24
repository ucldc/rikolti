import json
from .Fetcher import Fetcher
import requests
from urllib.parse import urlencode


class SolrFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(SolrFetcher, self).__init__(params)

        # If `next_url` is a param, we know that this is not the fetch of the
        # first page, so skip setting those attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        self.collection_id = params.get("collection_id")
        self.harvest_extra_data = params.get("harvest_data"). \
            get("harvest_extra_data")
        self.url = params.get("harvest_data").get("url").replace("http://",
                                                                 "https://")
        self.per_page = 100
        self.cursor_mark = "*"
        self.next_url = self.get_current_url()

    def get_current_url(self) -> str:
        """
        Returns the request URL.

        Returns: str
        """
        params = {
            "rows": self.per_page,
            "cursorMark": self.cursor_mark,
            "q": self.harvest_extra_data,
            "wt": "json",
            "sort": "id asc"
        }
        return self.url + "select?" + urlencode(params)

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

    def check_page(self, http_resp: requests.Response) -> int:
        """
        Parameters:
            http_resp: requests.Response

        Returns: bool
        """

        resp_dict = json.loads(http_resp.content)
        hits = len(resp_dict["response"]["docs"])

        print(
            f"[{self.collection_id}]: Fetched page {self.write_page} "
            f"at {http_resp.url} with {hits} hits"
        )

        return hits

    def increment(self, http_resp: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
             http_resp: requests.Response
        """
        super(SolrFetcher, self).increment(http_resp)

        previous_cursor_mark = self.cursor_mark
        resp_dict = json.loads(http_resp.content)
        self.cursor_mark = resp_dict["nextCursorMark"]

        self.next_url = self.get_current_url() if previous_cursor_mark != \
                                                  self.cursor_mark else None

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "harvest_extra_data": self.harvest_extra_data,
            "url": self.url,
            "collection_id": self.collection_id,
            "next_url": self.next_url,
            "write_page": self.write_page,
            "per_page": self.per_page,
            "cursor_mark": self.cursor_mark
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
