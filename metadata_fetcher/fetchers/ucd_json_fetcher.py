import json
from .Fetcher import Fetcher, FetchError
import requests
from xml.etree import ElementTree
from bs4 import BeautifulSoup
from metadata_fetcher import settings
import math


class UcdJsonFetcher(Fetcher):
    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(UcdJsonFetcher, self).__init__(params)

        self.collection_id = params.get("collection_id")
        self.url = params.get("harvest_data").get("url")
        self.per_page = 10

    def fetch_page(self) -> int:
        """
        Returns:
            int
        """
        page = {"url": self.url}
        print(f"[{self.collection_id}]: Fetching {page.get('url')}")
        try:
            response = requests.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch")

        return self.fetch_all_pages(response)

    def fetch_all_pages(self, response) -> int:
        """
        Parameters:
            response: Requests.response

        Returns:
            int
        """
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = ElementTree.fromstring(response.text)
        loc_nodes = xml.findall(".//ns:loc", ns)
        pages = math.ceil(len(loc_nodes) / self.per_page) - 1

        for page in range(pages):
            print(f"[{self.collection_id}]: Fetching URLs for page {page} "
                  f"({page + 1}/{pages + 1})")
            skip = self.write_page * self.per_page
            urls = loc_nodes[skip:(skip + self.per_page)]
            records = list(filter(None, [self.fetch_json_ld(url.text) for url in urls]))
            content = json.dumps(records)

            if settings.DATA_DEST.get("STORE") == "file":
                self.fetchtolocal(content)
            else:
                self.fetchtos3(content)

            self.write_page += 1
        return len(loc_nodes)

    def fetch_json_ld(self, url):
        """
        Takes a URL; fetches it and turns the contents of @seo-jsonld into a dict

        Parameters:
            url: string

        Returns:
            dict
        """

        try:
            response = requests.get(url=url)
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise FetchError(f"[{self.collection_id}]: unable to fetch {url}")

        soup = BeautifulSoup(response.text, "html.parser")
        json_ld_str = soup.find(id="seo-jsonld").text

        if json_ld_str:
            return json.loads(json_ld_str.strip())

    def json(self) -> str:
        """
        This fetcher is run once, then done

        Returns: str
        """
        return json.dumps({"finished": True})

