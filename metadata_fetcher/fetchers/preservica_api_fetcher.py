import json
import requests
from urllib.parse import urlsplit, quote

from .. import settings
from .Fetcher import Fetcher, FetchError, FetchedPageStatus, logger
from rikolti.utils.versions import put_versioned_page

class PreservicaApiFetcher(Fetcher):
    BASE_URL ="https://us.preservica.com/api"

    def __init__(self, params):
        """
        Parameters:
            params: dict[str]
        """
        super(PreservicaApiFetcher, self).__init__(params)

        self.harvest_data = params.get("harvest_data", {})
        self.url = self.harvest_data.get("url").replace("http://","https://")

        self.access_token = params.get("access_token")
        if not self.access_token:
            self.access_token = self.get_access_token()

        self.preservica_collection_id = params.get("preservica_collection_id")
        if not self.preservica_collection_id:
            self.preservica_collection_id = self.get_preservica_collection_id()

        self.num_fetched = params.get("num_fetched", 0)
        self.start_at = params.get("start_at", 0)

    def fetch_page(self) -> FetchedPageStatus:
        # get a page of child object ids
        object_children_response = self.get_object_children_page()
        children = object_children_response.json()
        object_ids = children.get("value", {}).get("objectIds", [])

        # Although it is possible to fetch object metadata via the
        # content/get_object_children endpoint, it only returns the first
        # value for each field, i.e. if a record has multiple subjects,
        # then it will only return the first one. Therefore, we now have
        # to hit the content/object-details endpoint for each record
        # to get full metadata.
        records = []
        record_count = 0
        for id in object_ids:
            object_detail_response = self.get_object_detail(id)
            item = object_detail_response.json()
            item = item.get("value", {})
            if item:
                records.append(item)
                record_count = record_count + 1

        if not record_count:
            logger.warning(
                f"[{self.collection_id}]: no records found "
                f"on page {self.write_page}"
            )

        filepath = None
        try:
            filepath = put_versioned_page(
                json.dumps(records), self.write_page, self.vernacular_version)
        except Exception as e:
            print(f"Metadata Fetcher: {e}")
            raise(e)

        self.increment(object_children_response)

        return FetchedPageStatus(record_count, filepath)

    def get_object_detail(self, id):
        # https://us.preservica.com/api/content/documentation.html#/%2F/get_object_details
        url = f"{self.BASE_URL}/content/object-details?id={quote(id)}"
        headers = {
                "Preservica-Access-Token": self.access_token,
                "accept": "application/json"
            }
        request = {"url": url, "headers": headers}

        try:
            response = self.http_session.get(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch object-details page {request}",
                f"Error was: {e}"
            )

        return response

    def get_object_children_page(self):
        # https://us.preservica.com/api/content/documentation.html#/%2F/get_object_children
        fields_to_fetch = [
            'id'
        ]
        url = (
                f"{self.BASE_URL}/content/object-children?"
                f"id={quote(self.preservica_collection_id)}"
                f"&q={quote('{}')}"
                f"&start={self.start_at}"
                f"&max=100"
                f"&metadata={quote(',').join(fields_to_fetch)}"
        )

        headers = {
                "Preservica-Access-Token": self.access_token,
                "accept": "application/json"
            }

        page = {
                "url": url,
                "headers": headers
            }

        try:
            response = self.http_session.get(**page)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FetchError(
                f"[{self.collection_id}]: unable to fetch object-children page {page}",
                f"Error was: {e}"
            )

        return response

    def get_preservica_collection_id(self) -> str:
        # example harvest url:
        # https://oakland.access.preservica.com/uncategorized/SO_46d67cb7-caad-4d3d-aec1-2fef7c7e7ae7/
        path = urlsplit(self.url).path.strip('/')
        id = path.split('/')[-1]
        if not id.startswith('SO_'):
            raise FetchError(
                    f"[{self.collection_id}]: invalid ID provided: {id}"
                    f"ID must start with 'SO_'"
                )

        return id.replace("SO_", "sdb:SO|")

    def get_access_token(self) -> str:
        """
        Get access token: https://us.preservica.com/api/accesstoken/documentation.html#//post_login
        """
        url = f"{self.BASE_URL}/accesstoken/login"
        data = {
            "username": settings.PRESERVICA_USER,
            "password": settings.PRESERVICA_PASS,
            "cookie": "false",
            "includeUserDetails": "false"
        }

        headers = {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        request = {
            "url": f"{self.BASE_URL}/accesstoken/login",
            "data": data,
            "headers": headers
        }
        try:
            response = self.http_session.post(**request)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FetchError(
                f"[{self.collection_id}]: unable to get api access token from {url}",
                f"Error was: {e}"
            )

        token = response.json().get("token")
        if not token:
            raise FetchError(
                f"[{self.collection_id}]: unable to get api access token from {url}"
                f"Response was: {response}"
            )

        return token

    def increment(self, http_resp):
        self.write_page = self.write_page + 1

        fetched_page = http_resp.json()
        self.num_fetched += len(fetched_page.get("value", {}).get("metadata", []))
        if self.num_fetched >= fetched_page.get("value", {}).get("totalHits", 0):
            self.finished = True
        else:
            self.finished = False

        self.start_at = self.num_fetched

    def json(self) -> str:
        return json.dumps({
            "finished": self.finished,
            "harvest_type": self.harvest_type,
            "collection_id": self.collection_id,
            "harvest_data": self.harvest_data,
            "write_page": self.write_page,
            "access_token": self.access_token,
            "preservica_collection_id": self.preservica_collection_id,
            "num_fetched": self.num_fetched,
            "start_at": self.start_at
        })
