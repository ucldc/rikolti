import json
import math
import sys

from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from xml.etree import ElementTree
from bs4 import BeautifulSoup

from .Fetcher import Fetcher, FetchError, FetchedPageStatus
from rikolti.utils.versions import put_versioned_page

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

    def fetch_page(self) -> list[FetchedPageStatus]:
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

    def fetch_all_pages(
            self, response: requests.Response) -> list[FetchedPageStatus]:
        """
        Parameters:
            response: requests.Response

        Returns:
            list[FetchedPageStatus]
        """
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        xml = ElementTree.fromstring(response.text)
        loc_nodes = xml.findall(".//ns:loc", ns)
        pages = math.ceil(len(loc_nodes) / self.per_page)

        fetch_status = []
        for page in range(pages):
            print(f"[{self.collection_id}]: Fetching URLs for page {page + 1} "
                  f"({page + 1}/{pages})")
            offset = self.write_page * self.per_page
            urls = loc_nodes[offset:(offset + self.per_page)]
            urls = list(filter(None, [url.text for url in urls]))
            records = [self.fetch_json_ld(url) for url in urls]
            document_count = len(records)
            try:
                filepath = put_versioned_page(
                    json.dumps(records), self.write_page, self.vernacular_version)
                fetch_status.append(FetchedPageStatus(document_count, filepath))
            except Exception as e:
                print(f"Metadata Fetcher: {e}", file=sys.stderr)
                raise(e)
            self.write_page += 1
        return fetch_status

    def fetch_json_ld(self, url: str) -> Optional[dict]:
        """
        Takes a URL; fetches it and turns the contents of @seo-jsonld into a dict

        Parameters:
            url: str

        Returns:
            dict
        """
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(url=url)
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

