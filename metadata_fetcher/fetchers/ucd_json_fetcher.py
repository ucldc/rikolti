import json
import sys
from .Fetcher import Fetcher, FetchError
import requests
from xml.etree import ElementTree
from bs4 import BeautifulSoup
from .. import settings
import math
from typing import Optional


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
        UCD's harvesting endpoint gets us an XML document listing a URL for every record
        in a collection, but not the actual metadata records themselves. fetch_page
        fetches this initial XML document, while fetch_all_pages below batches the
        records in sets of 10 (self.per_page) and fetch_json_ld fetches individual
        records.

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

    def fetch_all_pages(self, response: requests.Response) -> int:
        """
        Parameters:
            response: requests.Response

        Returns:
            int
        """
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = ElementTree.fromstring(response.text)
        loc_nodes = xml.findall(".//ns:loc", ns)
        pages = math.ceil(len(loc_nodes) / self.per_page)

        for page in range(pages):
            print(f"[{self.collection_id}]: Fetching URLs for page {page + 1} "
                  f"({page + 1}/{pages})")
            skip = self.write_page * self.per_page
            urls = loc_nodes[skip:(skip + self.per_page)]
            records = list(filter(None, [self.fetch_json_ld(url.text) for url in urls]))
            content = json.dumps(records)

            try:
                self.data_destination.put_page_content(
                    content, relative_path=(
                        f"{self.collection_id}/vernacular_metadata/{self.write_page}"
                    )
                )
            except Exception as e:
                print(f"Metadata Fetcher: {e}", file=sys.stderr)
                raise(e)

            self.write_page += 1
        return len(loc_nodes)

    def fetch_json_ld(self, url: str) -> Optional[dict]:
        """
        Takes a URL; fetches it and turns the contents of @seo-jsonld into a dict

        Parameters:
            url: str

        Returns:
            dict
        """
        response = self.make_http_request(url)
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

