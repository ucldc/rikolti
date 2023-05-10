import json
import requests
from urllib.parse import urlencode
from xml.etree import ElementTree
from .Fetcher import Fetcher


class OacFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(OacFetcher, self).__init__(params)

        # If `next_url` is a param, we know that this is not the fetch of the
        # first page, so skip setting those attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        harvest_data = params.get("harvest_data")
        self.url = harvest_data.get("url")
        self.harvest_extra_data = harvest_data.get("harvest_extra_data")
        self.collection_id = params.get("collection_id")
        self.per_page = 100
        self.next_url = self.get_current_url()

    def get_current_url(self) -> str:
        """
        Returns: str
        """
        params = {
            "docsPerPage": self.per_page,
            "startDoc": 1 + (self.write_page * self.per_page)
        }

        base_url = self.url

        # TODO: remove this after the URLs have changed in registry
        base_url = base_url.replace("facet=type-tab&", "")

        return base_url + "&" + urlencode(params)

    def build_fetch_request(self: dict[str]):
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        request = {"url": self.get_current_url()}

        return request

    def check_page(self, http_resp: requests.Response):
        """
        Parameters:
            http_resp: requests.Response

        Returns: bool
        """
        xml_resp = ElementTree.fromstring(http_resp.content)
        start_doc = int(xml_resp.get("startDoc"))
        end_doc = int(xml_resp.get("endDoc"))

        print(
            f"[{self.collection_id}]: Fetched page "
            f"at {http_resp.url} "
            f"with {end_doc - start_doc} hits"
        )

        return start_doc != end_doc

    def increment(self, http_resp: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
             http_resp: requests.Response
        """
        super(OacFetcher, self).increment(http_resp)

        xml_resp = ElementTree.fromstring(http_resp.content)
        total_docs = int(xml_resp.get("totalDocs"))
        end_doc = int(xml_resp.get("endDoc"))

        self.next_url = self.get_current_url() if end_doc < total_docs else None

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
            "per_page": self.per_page
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
