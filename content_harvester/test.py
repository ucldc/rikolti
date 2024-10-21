import requests
from requests.adapters import HTTPAdapter, Retry
from typing import Optional, Any, Tuple
from dataclasses import dataclass, asdict
from urllib.parse import urlparse

def configure_http_session() -> requests.Session:
    http = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[413, 429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http
http_session = configure_http_session()

@dataclass(frozen=True)
class ContentRequest(object):
    """
    An immutable, hashable object representing a request for content.
    This is used as the cache key in download_url. Since auth can be
    Any, we need to define our own __hash__ and __eq__ methods for use
    with the lru cache.
    """
    url: str
    auth: Any

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, other):
        return self.url == other.url

    def __post_init__(self):
        if self.auth and urlparse(self.url).scheme != 'https':
            raise Exception(
                f"Basic auth not over https is a bad idea! {self.url}")

# ContentRequest(url='https://nuxeo.cdlib.org/nuxeo/nxfile/default/9eb519ed-7baf-4da5-915f-3570ae2bfd04/file:content/Don%20Pedro%20TCP%20Study%20Report%20Final_April%202015.pdf', auth=('Administrator', 'cable8:ringmasters'))
media_source_url = 'https://nuxeo.cdlib.org/nuxeo/nxfile/default/9eb519ed-7baf-4da5-915f-3570ae2bfd04/file:content/Don%20Pedro%20TCP%20Study%20Report%20Final_April%202015.pdf'
auth = ('Administrator', 'cable8:ringmasters')
request = ContentRequest(media_source_url, auth)
#mimicked_head_request = {**asdict(request), 'headers': {"Range": "bytes=0-0"}}
#print(mimicked_head_request['auth'])

#print(mimicked_head_request)
#print(type(mimicked_head_request))
#head_resp = http_session.get(mimicked_head_request)
#head_resp = http_session.head(**asdict(request))
head_resp = http_session.get(**asdict(request), headers={"Range": "bytes=0-0"})
print(f"{head_resp.headers}")

#resp = requests.get(url=url, auth=auth, headers={"Range": "bytes=0-0"})