import json
from .Fetcher import Fetcher
import requests
from urllib.parse import urlencode


class InternetArchiveFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(InternetArchiveFetcher, self).__init__(params)

        self.harvest_data = params.get("harvest_data", {})
        self.harvest_extra_data = self.harvest_data.get("harvest_extra_data")
        self.url = self.harvest_data.get("url").replace("http://","https://")
        self.current_page = params.get("current_page", 1)
        self.num_fetched = params.get("num_fetched", 0)

    def build_fetch_request(self) -> dict[str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        params = {
            "rows": 100,
            "q": self.harvest_extra_data,
            "output": "json",
            "sort": "identifier asc",
            "page": self.current_page
        }
        url = f"{self.url}?{urlencode(params)}"
        request = {"url": url}

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
        super(InternetArchiveFetcher, self).increment(http_resp)
        
        resp_dict = http_resp.json()
        self.num_found = resp_dict["response"]["numFound"]
        self.num_fetched = self.num_fetched + len(resp_dict["response"]["docs"])
        if self.num_fetched >= self.num_found:
            self.finished = True
        else:
            self.finished = False

        self.current_page += 1

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "write_page": self.write_page,
            "harvest_data": self.harvest_data,
            "current_page": self.current_page,
            "num_fetched": self.num_fetched,
            "finished": self.finished
        }

        return json.dumps(current_state)
