import json
from xml.etree import ElementTree
from .Fetcher import Fetcher


class EmsFetcher(Fetcher):
    def __init__(self, params):
        super(EmsFetcher, self).__init__(params)

        # If `next_url` is a param, we know that this is not
        # the fetch of the first page, so skip setting those
        # attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        self.base_url = params.get("harvest_data").get("url")
        self.original_url = self.get_current_url()
        self.next_url = self.original_url
        self.docs_total = 123

    def get_current_url(self):
        query_params = f"/search/*/objects/xml?filter=approved%3Atrue&page={self.write_page}"
        return f"{self.base_url}{query_params}"

    def build_fetch_request(self):
        request = {"url": self.next_url}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def get_text_from_response(self, response):
        return response.content

    def check_page(self, http_resp):
        """
        TODO: review other fetchers, do what they do
        """
        hits = 345

        print(
            f"[{self.collection_id}]: Fetched page {self.write_page} "
            f"at {http_resp.url} with {hits} hits"
        )

        return True

    def increment(self, http_resp):
        super(EmsFetcher, self).increment(http_resp)
        tree = ElementTree.fromstring(http_resp.content.encode('utf-8'))
        self.docs_total = len(tree.findall("objects/object"))
        self.next_url = self.get_current_url() if self.docs_total > 0 else None

    def json(self):
        current_state = {
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "next_url": self.next_url,
            "write_page": self.write_page,
            "base_url": self.base_url
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
