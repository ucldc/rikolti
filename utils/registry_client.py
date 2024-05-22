from urllib.parse import urlparse, parse_qs
import requests

def registry_endpoint(url):
    if parse_qs(urlparse(url).query).get('format') != ['json']:
        raise KeyError("registry_client requires urls with format=json")

    page = url
    while page:
        response = requests.get(url=page)
        response.raise_for_status()
        page = response.json().get('meta', {}).get('next', None)
        if page:
            page = f"https://registry.cdlib.org{page}"

        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            yield collection
