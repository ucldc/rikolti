import requests
from requests.adapters import HTTPAdapter, Retry

def configure_http_session() -> requests.Session:
    http = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[413, 429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    http.headers.update({
        "User-Agent": "Calisphere metadata fetcher: github.com/ucldc/rikolti"
    })
    return http

