import importlib
import json
import logging
import sys

from datetime import datetime
from dataclasses import dataclass

from .fetchers.Fetcher import Fetcher, FetchedPageStatus
from rikolti.utils.versions import create_vernacular_version, get_version

logger = logging.getLogger(__name__)

def import_fetcher(harvest_type):
    fetcher_module = importlib.import_module(
        f".fetchers.{harvest_type}_fetcher", package=__package__)
    fetcher_module_words = harvest_type.split('_')
    class_type = ''.join([word.capitalize() for word in fetcher_module_words])
    fetcher_class = getattr(fetcher_module, f"{class_type}Fetcher")
    if fetcher_class not in Fetcher.__subclasses__():
        raise Exception(
            f"Fetcher class {fetcher_class} not a subclass of Fetcher")
    return fetcher_class


@dataclass(frozen=True, eq=True)
class FetchedCollectionStatus:
    num_items: int
    num_pages: int
    num_parent_items: int
    num_parent_pages: int
    filepaths: list[str]
    children: bool
    version: str


def aggregate_page_statuses(
        collection_id, 
        statuses: list[FetchedPageStatus]) -> FetchedCollectionStatus:
    total_items = sum([status.document_count for status in statuses])
    parent_items = total_items
    total_pages = len(statuses)
    parent_pages = total_pages
    filepaths = [status.vernacular_filepath for status in statuses]
    version = get_version(collection_id, filepaths[0])
    children = False
    for status in statuses:
        child_page_statuses = status.children
        if child_page_statuses:
            child_status = aggregate_page_statuses(collection_id, child_page_statuses)
            total_items = total_items + child_status.num_items
            total_pages = total_pages + child_status.num_pages
            filepaths = filepaths + child_status.filepaths
            children = True
    return FetchedCollectionStatus(
        num_items=total_items,
        num_pages=total_pages,
        num_parent_items=parent_items,
        num_parent_pages=parent_pages,
        filepaths = filepaths,
        children = children,
        version = version
    )


# AWS Lambda entry point
def fetch_collection(payload, vernacular_version) -> FetchedCollectionStatus:
    """
    returns a FetchedCollectionStatus objects with the following attributes:
        num_items: int
        num_pages: int
        num_parent_items: int
        num_parent_pages: int
        filepaths: list[str]
            ex: ["3433/vernacular_version_1/data/1"]
        children: bool
        version: str
            ex: "3433/vernacular_version_1"
    """
    if isinstance(payload, str):
        payload = json.loads(payload)

    logger.debug(f"fetch_collection payload: {payload}")

    fetcher_class = import_fetcher(payload.get('harvest_type'))
    payload.update({'vernacular_version': vernacular_version})
    next_page = payload
    page_statuses = []

    while not next_page.get('finished'):
        fetcher = fetcher_class(next_page)
        page_status = fetcher.fetch_page()
        page_statuses.append(page_status)

        # this is a nuxeo and ucd json fetcher workaround
        if len(page_statuses) == 1 and isinstance(page_statuses[0], list):
            page_statuses = page_statuses[0]

        next_page = json.loads(fetcher.json())
        next_page.update({'vernacular_version': vernacular_version})

    fetched_collection_status = aggregate_page_statuses(
        payload.get('collection_id'), page_statuses)

    return fetched_collection_status


def print_fetched_collection_report(collection, fetched_collection):
    diff_items = (
        fetched_collection.num_parent_items - collection.get('solr_count', 0))
    diff_items_label = ""
    if diff_items > 0:
        diff_items_label = 'new items'
    elif diff_items < 0:
        diff_items_label = 'lost items'
    else:
        diff_items_label = 'same extent'

    date = "registry has no record of Solr count"
    if collection.get('solr_last_updated'):
        date = datetime.strptime(
            collection['solr_last_updated'],
            "%Y-%m-%dT%H:%M:%S.%f"
        )
        date = datetime.strftime(date, '%B %d, %Y %H:%M:%S.%f')

    fetch_report_row = (
        f"{collection['id']:<6}: {'success,':9} "
        f"{fetched_collection.num_pages:>4} pages, "
        f"{fetched_collection.num_items:>6} items, "
        f"{collection.get('solr_count', 0):>6} solr items, "
        f"{str(diff_items) + ' ' + diff_items_label + ',':>16} "
        f"solr count last updated: {date}"
    )
    if fetched_collection.children:
        num_children = (
            fetched_collection.num_items - fetched_collection.num_parent_items)
        num_children_pages = (
            fetched_collection.num_pages - fetched_collection.num_parent_pages)
        print(
            f"{fetched_collection.num_parent_items} parent items "
            f"{fetched_collection.num_parent_pages} parent pages "
            f"{num_children} child items "
            f"{num_children_pages} child pages"
        )

    print(fetch_report_row)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Fetch metadata in the institution's vernacular")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    payload = json.loads(args.payload)

    logging.basicConfig(
        filename=f"fetch_collection_{payload.get('collection_id')}.log",
        encoding='utf-8',
        level=logging.DEBUG
    )
    vernacular_version = create_vernacular_version(payload.get('collection_id'))
    print(f"Starting to fetch collection {payload.get('collection_id')}")
    fetch_collection(payload, vernacular_version)
    print(f"Finished fetching collection {payload.get('collection_id')}")
    sys.exit(0)
