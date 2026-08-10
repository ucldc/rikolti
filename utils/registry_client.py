import os
from urllib.parse import parse_qs, urlparse

import requests

try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

if AIRFLOW_AVAILABLE:
    registry_auth = Variable.get("rikolti_registry_auth", deserialize_json=True)
    REGISTRY_USER = registry_auth.get('user', '')
    REGISTRY_TOKEN = registry_auth.get('token', '')
else:
    REGISTRY_USER = os.environ.get('RIKOLTI_REGISTRY_USER', '')
    REGISTRY_TOKEN = os.environ.get('RIKOLTI_REGISTRY_TOKEN', '')


def collection_count(url):
    response = requests.get(
        url=url,
        headers={
            "Authorization": f"ApiKey {REGISTRY_USER}:{REGISTRY_TOKEN}"
        }
    )
    response.raise_for_status()
    total = response.json().get('meta', {}).get('total_count', 1)
    return total


def registry_endpoint(url):
    if parse_qs(urlparse(url).query).get('format') != ['json']:
        raise KeyError("registry_client requires urls with format=json")

    page = url
    while page:
        response = requests.get(
            url=page,
            headers={
                "Authorization": f"ApiKey {REGISTRY_USER}:{REGISTRY_TOKEN}"
            }
        )

        response.raise_for_status()
        page = response.json().get('meta', {}).get('next', None)
        if page:
            page = f"https://registry.cdlib.org{page}"

        collections = response.json().get('objects', [response.json()])
        for collection in collections:  # noqa: UP028
            yield collection
