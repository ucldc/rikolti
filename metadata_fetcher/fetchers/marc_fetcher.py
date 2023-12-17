import requests

from .Fetcher import Fetcher
import json
import pymarc


class MarcFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        super(MarcFetcher, self).__init__(params)
        self.url = params.get("harvest_data").get("url")

    def build_fetch_request(self) -> dict[str]:
        return {"url": self.url}

    def check_page(self, http_resp: requests.Response) -> int:
        return sum(1 for _ in pymarc.MARCReader(http_resp.content,
                                                to_unicode=True,
                                                utf8_handling="replace"))

    def json(self) -> str:
        return json.dumps({"finished": True})

