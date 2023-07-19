import json
from .Fetcher import Fetcher, FetchError
import requests
from xml.etree import ElementTree
import settings
import math

class XmlFileFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(XmlFileFetcher, self).__init__(params)

        self.collection_id = params.get("collection_id")
        self.url = params.get("harvest_data").get("url")
        self.per_page = 100

    def fetch_page(self) -> int:
        """
        Returns:
            int
        """
        page = {"url": self.url}
        print(
            f"[{self.collection_id}]: Fetching {page.get('url')}"
        )
        try:
            response = requests.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch ")

        return self.fetch_all_pages(response)

    def fetch_all_pages(self, response) -> int:
        """
        Parameters:
            response: Requests.response

        Returns:
            int
        """
        xml = ElementTree.fromstring(response.text)
        record_nodes = xml.findall(".//record")
        pages = math.ceil(len(record_nodes) / self.per_page)

        for page in range(pages):
            skip = self.write_page * self.per_page
            items = record_nodes[skip:(skip + self.per_page)]
            content = "<records>" + \
                      "".join([ElementTree.tostring(item, encoding="unicode")
                               for item in items]) + "</records>"
            if settings.DATA_DEST == 'local':
                self.fetchtolocal(content)
            else:
                self.fetchtos3(content)
            self.write_page += 1
        return len(record_nodes)

    def json(self) -> str:
        """
        This fetcher is run once, then done

        Returns: str
        """
        return json.dumps({"finished": True})

