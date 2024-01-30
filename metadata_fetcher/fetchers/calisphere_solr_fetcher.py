import json
import requests

from .Fetcher import Fetcher
from ..settings import CALISPHERE_ETL_TOKEN

class CalisphereSolrFetcher(Fetcher):
    def __init__(self, params: dict[str, str]):
        super(CalisphereSolrFetcher, self).__init__(params)
        self.collection_id = params.get("collection_id")
        self.cursor_mark = params.get("cursor_mark", "*")
        self.num_found = params.get("num_found", 0)
        self.num_fetched = params.get("num_fetched", 0)

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
            "url": "https://solr.calisphere.org/solr/query",
            "headers": {'X-Authentication-Token': CALISPHERE_ETL_TOKEN},
            "params": params
        }

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')} with params {params}")

        return request

    def check_page(self, http_resp: requests.Response) -> int:
        """
        Parameters:
            http_resp: requests.Response

        Returns: int: number of records in the response
        """

        resp_dict = http_resp.json()
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
        resp_dict = http_resp.json()

        # this is a workaround for solr giving us an extra page
        # with zero docs after the last page of results
        self.num_found = resp_dict["response"]["numFound"]
        self.num_fetched = self.num_fetched + len(resp_dict["response"]["docs"])
        if self.cursor_mark != resp_dict["nextCursorMark"] \
        and self.num_fetched != self.num_found:
            self.cursor_mark = resp_dict["nextCursorMark"]
            self.finished = False
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
            "cursor_mark": self.cursor_mark,
            "num_found": self.num_found,
            "num_fetched": self.num_fetched,
            "finished": self.finished
        }

        return json.dumps(current_state)
