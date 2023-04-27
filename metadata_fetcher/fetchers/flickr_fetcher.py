import json
from .Fetcher import Fetcher
import requests
from requests.adapters import HTTPAdapter
from requests.adapters import Retry
from urllib.parse import urlencode
import settings


class FlickrFetcher(Fetcher):
    BASE_URL: str = "https://api.flickr.com/services/rest/"

    def __init__(self, params: dict[str]):
        """
        Parameters:
            params: dict[str]
        """
        super(FlickrFetcher, self).__init__(params)

        # Tracks where we're at processing photo requests
        self.photo_index = 1
        self.photo_total = 0

        # If `next_url` is a param, we know that this is not the fetch of the
        # first page, so skip setting those attributes
        if "next_url" in params:
            for key in params:
                setattr(self, key, params[key])
            return

        self.collection_id = params.get("collection_id")
        self.harvest_extra_data = params.get("harvest_data").\
            get("harvest_extra_data")
        self.per_page = 100
        self.next_url = self.get_current_url()

    def get_current_url(self) -> str:
        """
        Returns the request URL based on the type of collection, photoset or
        user_photos. Note we increment the page number since Flickr starts
        with page 1 and internal page iteration starts with page 0.

        Returns: str
        """
        params = {
            "per_page": self.per_page,
            "page": self.write_page + 1
        }

        if self.is_photoset():
            params.update(self.get_request_method_params("photosets.getPhotos"))
            params.update({"photoset_id": self.harvest_extra_data})
            return self.build_request_url(params)

        params.update(self.get_request_method_params("people.getPublicPhotos"))
        params.update({"user_id": self.harvest_extra_data})
        return self.build_request_url(params)

    def is_photoset(self) -> bool:
        """
        Based on the harvest_extra_data, determines if it's a photoset vs user
        photos.

        Returns: bool
        """
        return "@N" not in self.harvest_extra_data

    def get_request_method_params(self, method: str) -> dict[str]:
        """
        Generates the base set of parameters for all Flickr API requests

        Parameters:
            method: str

        Returns: dict[str]
        """
        return {
            "api_key": settings.FLICKR_API_KEY,
            "method": f"flickr.{method}",
            "format": "json",
            "nojsoncallback": "1"
        }

    def build_request_url(self, params: dict[str]) -> str:
        """
        Creates a Flickr API request URL from dictionary of request parameters.

        Parameters:
            params: dict[str]

        Returns: str
        """
        return self.BASE_URL + "?" + urlencode(params)

    def build_fetch_request(self) -> dict[str]:
        """
        Generates arguments for `requests.get()`.

        Returns: dict[str]
        """
        request = {"url": self.get_current_url()}

        print(
            f"[{self.collection_id}]: Fetching page {self.write_page} "
            f"at {request.get('url')}")

        return request

    def aggregate_vernacular_content(self, content: str) -> str:
        """
        Accepts a content from a response for page of photos, and transforms it
        in a dictionary. This requires a `flickr.photos.getInfo` request for
        each photo.

        Parameters:
            content: str

        Returns: str
        """
        photos = json.loads(content)

        print(
            f"[{self.collection_id}]: Starting to fetch all photos for page"
            f" {self.write_page}"
        )

        photo_data = []
        for photo in photos.get(self.response_items_attribute, {}).\
                get("photo", []):
            content = self.get_photo_metadata(photo.get("id")).content
            photo_data.append(json.loads(content).get("photo"))
            self.photo_index += 1

        print(
            f"[{self.collection_id}]: Fetched all photos for page"
            f" {self.write_page}"
        )

        return json.dumps(photo_data)

    def get_photo_metadata(self, id: str) -> requests.Response:
        """
        Performs a request for photo info and returns the response. Attempts
        retries.

        Parameters:
            id: str

        Returns: requests.Response
        """
        params = self.get_request_method_params("photos.getInfo")
        params.update({
            "photo_id": id
        })
        url = self.build_request_url(params)

        print(
            f"[{self.collection_id}]: Fetching photo {id} "
            f"({self.photo_index} of {self.photo_total}) at {url}"
        )

        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2)
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session.get(url=url)

    def check_page(self, http_resp: requests.Response) -> bool:
        """
        Parameters:
            http_resp: requests.Response

        Returns: bool
        """
        data = json.loads(http_resp.content)
        self.photo_total = len(data.get(self.response_items_attribute, {}).
                               get("photo", []))

        print(
            f"[{self.collection_id}]: Fetched ids for page {self.write_page} "
            f"at {http_resp.url} with {self.photo_total} hits"
        )

        return True

    def increment(self, http_resp: requests.Response):
        """
        Sets the `next_url` to fetch and increments the page number.

        Parameters:
             http_resp: requests.Response
        """
        super(FlickrFetcher, self).increment(http_resp)

        data = json.loads(http_resp.content)
        pagination = data.get(self.response_items_attribute, {})
        page = int(pagination.get("page", 0))
        pages = int(pagination.get("pages", 0))

        self.next_url = self.get_current_url() if page < pages else None

    @property
    def response_items_attribute(self) -> str:
        """
        The API returns photos wrapped in an object named based on whether it's
        returning a photoset or a user's photos

        Returns: str
        """
        return "photoset" if self.is_photoset() else "photos"

    def json(self) -> str:
        """
        Generates JSON for the next page of results.

        Returns: str
        """
        current_state = {
            "harvest_type": self.harvest_type,
            "harvest_extra_data": self.harvest_extra_data,
            "collection_id": self.collection_id,
            "next_url": self.next_url,
            "write_page": self.write_page,
            "per_page": self.per_page
        }

        if not self.next_url:
            current_state.update({"finished": True})

        return json.dumps(current_state)
