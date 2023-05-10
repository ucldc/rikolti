import requests
import logging

def registry_list(url, limit=None):
    """
    Generator that yields collections from a registry endpoint
    """
    collection_page = url
    count = 0
    while collection_page:
        response = requests.get(url=collection_page)
        response.raise_for_status()
        total_collections = response.json().get('meta', {}).get('total_count', 1)
        current_collections = response.json().get('meta', {}).get('limit', 0)
        logging.debug(
            f">>> Getting registry data for {current_collections} of "
            f"{total_collections} collections described at {collection_page}"
        )
        collection_page = response.json().get('meta', {}).get('next')
        if collection_page:
            collection_page = f"https://registry.cdlib.org{collection_page}"
        collections = response.json().get('objects', [response.json()])
        for collection in collections:
            yield collection
            count +=1
            if limit and count >= limit:
                break
        if limit and count >= limit:
            break
        logging.debug(f"Next page: {collection_page}")
