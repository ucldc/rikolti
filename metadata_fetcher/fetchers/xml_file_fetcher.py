import json
import logging
import requests
from xml.etree import ElementTree

from .Fetcher import Fetcher, FetchedPageStatus, FetchError
from rikolti.utils.versions import put_versioned_page

logger = logging.getLogger(__name__)


class XmlFileFetcher(Fetcher):

    def __init__(self, params: dict[str]):
        super(XmlFileFetcher, self).__init__(params)

        self.url = params.get("harvest_data").get("url")

    def fetch_page(self) -> list[FetchedPageStatus]:
        page = self.build_fetch_request()
        logger.debug(
            f"[{self.collection_id}]: fetching page at {page.get('url')}"
        )
        try:
            response = self.http_session.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch from {page.get('url')}",
                f"Error was: {e}"
            )

        record_count = self.check_page(response)
        if not record_count:
            logger.warning(
                f"[{self.collection_id}]: no records found"
            )

        return self.write_pages(response)

    def write_pages(self, response) -> list[FetchedPageStatus]:
        """
        # batch into pages of 100 records each
        # NOTE: This arguably goes against the principle of the fetcher not making any
        #       changes to fetched data, but if we don't batch the records here, then it
        #       causes the content harvester to fail due to excessively large payloads
        # TODO: in python3.12 we can use itertools.batched
        """
        xml = ElementTree.fromstring(response.text)
        record_nodes = xml.findall(".//record")

        batch_size = 100
        pages = []
        write_page = 0
        for i in range(0, len(record_nodes), batch_size):
            items = record_nodes[i:i+batch_size]
            content = "<records>" + \
                      "".join([ElementTree.tostring(item, encoding="unicode")
                               for item in items]) + "</records>"
            try:
                filepath = put_versioned_page(
                    content, write_page, self.vernacular_version)
            except Exception as e:
                print(f"Metadata Fetcher: {e}")
                raise(e)

            write_page += 1
            pages.append(FetchedPageStatus(len(items), filepath))

        return pages

    def build_fetch_request(self):
        return {"url": self.url}

    def check_page(self, response):
        """
        This check assumes no xml namespace for record elements
        """
        xml_resp = ElementTree.fromstring(response.text)
        xml_hits = xml_resp.findall(".//record")

        if len(xml_hits) > 0:
            logging.debug(
                f"{self.collection_id}, fetched xml file - "
                f"{len(xml_hits)} hits,-,-,-,-,-"
            )
        return len(xml_hits)

    def json(self) -> str:
        return json.dumps({"finished": True})
