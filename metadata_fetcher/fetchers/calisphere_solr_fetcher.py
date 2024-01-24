import json
from .Fetcher import Fetcher
import requests
from urllib.parse import urlencode


class CalisphereSolrFetcher(Fetcher):
    def __init__(self, params: dict[str, str]):
        super(CalisphereSolrFetcher, self).__init__(params)
        self.collection_id = params.get("collection_id")
        self.cursor_mark = params.get("cursor_mark", "*")

    def build_fetch_request(self) -> dict[str, str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str, str]
        """
        params = {
            "fq": (
                "collection_url:\"https://registry.cdlib.org/api/v1/"
                f"collection/{self.collection_id}/\""
            ),
            "rows": 100,
            "cursorMark": self.cursor_mark,
            "wt": "json",
            "sort": "id asc"
        }

        request = {
            "url": "https://solr.calisphere.org/solr/select?" + urlencode(params)
        }

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def check_page(self, http_resp: requests.Response) -> int:
        """
        Parameters:
            http_resp: requests.Response

        Returns: int: number of records in the response
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
        super(CalisphereSolrFetcher, self).increment(http_resp)
        resp_dict = json.loads(http_resp.content)
        if self.cursor_mark != resp_dict["nextCursorMark"]:
            self.cursor_mark = resp_dict["nextCursorMark"]
        else:
            self.finished = True

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "cursor_mark": self.cursor_mark
        }

        if self.finished:
            current_state.update({"finished": True})

        return json.dumps(current_state)
