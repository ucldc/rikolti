import os
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

REGISTRY_AUTH: dict = {}
if AIRFLOW_AVAILABLE:
    REGISTRY_AUTH = Variable.get(
        "rikolti_registry_auth", deserialize_json=True, default_var={}
    )
else:
    REGISTRY_AUTH = {
        "username": os.environ.get('RIKOLTI_REGISTRY_USER', ''),
        "api_token": os.environ.get('RIKOLTI_REGISTRY_TOKEN', '')
    }

if (
    set(REGISTRY_AUTH.keys()) != {'username', 'api_token'} or
    not all(REGISTRY_AUTH.values())
):
    REGISTRY_AUTH = {}
    # uncomment after migration complete to help developers catch missing credentials
    # in dev environments.
    # raise ValueError(
    #     "Registry credentials are not set. Please set the "
    #     "RIKOLTI_REGISTRY_USER and RIKOLTI_REGISTRY_TOKEN environment "
    #     "variables or the rikolti_registry_auth Airflow variable."
    # )

def add_auth(url):
    if REGISTRY_AUTH:
        # add username and api_key as get parameters
        url_parts = urlparse(url)
        qs = dict(parse_qs(url_parts.query))
        qs.update(REGISTRY_AUTH)
        url_parts = list(url_parts)
        url_parts[4] = urlencode(qs, doseq=True)
        return str(urlunparse(url_parts))
    return url


def mapper(collection_id):
    url = ("https://registry.cdlib.org/api/v1/rikoltimapper/"
           f"{collection_id}/?format=json")
    try:
        response = requests.get(url=add_auth(url))
        response.raise_for_status()
        collection_data = response.json()
    except requests.exceptions.HTTPError as err:
        print(
            f"[Collection {collection_id}]: "
            f"[{url}]"
            f"{err}; Can't retrieve collection data from registry"
        )
    return collection_data


def collection(collection_id):
    resp = requests.get(
        add_auth(
            f'https://registry.cdlib.org/api/v1/'
            f'rikolticollection/{collection_id}/?format=json'
        )
    )
    resp.raise_for_status()
    collection = resp.json()
    return collection


def collection_count(url):
    response = requests.get(url=add_auth(url))
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    return total


def registry_endpoint(url):
    if parse_qs(urlparse(url).query).get('format') != ['json']:
        raise KeyError("registry_client requires urls with format=json")

    page = url
    while page:
        response = requests.get(url=add_auth(page))

        response.raise_for_status()
        page = response.json().get('meta', {}).get('next', None)
        if page:
            page = f"https://registry.cdlib.org{page}"

        collections = response.json().get('objects', [response.json()])
        for collection in collections:  # noqa: UP028
            yield collection
