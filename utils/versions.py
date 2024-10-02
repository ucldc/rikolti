import json
import os
from datetime import datetime
from typing import Union, Optional
from . import storage

"""
This module works with "version paths" and "version pages"

If s3://bucket-name/3433/vernacular_data_v1/data/page1.xml is the URI to the
first page of vernacular data, then 3433/vernacular_data_v1/ is the "version"
or "version path" and 3433/vernacular_data_v1/data/page1.xml is the
"version page". A version path always starts with the collection id.

This module implements the creation of new version paths given an existing
version path, a method to find a version path in an arbitrary string - usually
an absolute path URI:
"""

def get_version(collection_id: Union[int, str], uri: str) -> str:
    """
    Takes an arbitrary string (usually a URI) and tries to find a version path
    by splitting on the collection id and discarding everything prior to the
    collection ID and also discarding everything after the special "data"
    keyword.

    Returns a version path.
    Test cases we've encountered: "8/vernacular_metadata_2024-01-31T00:39:58/data/986", "8/vernacular_metadata_v1/data/8"
    """
    collection_id = str(collection_id)
    uri_parts = uri.strip('/').split('/')
    if str(collection_id) not in uri_parts or len(uri_parts) < 2:
        raise Exception(f"Not a valid version path: {uri}, {uri_parts}")
    path_list = uri_parts[uri_parts.index(collection_id):]
    if 'data' in path_list:
        path_list = path_list[:path_list.index('data')]
    version = "/".join(path_list)
    return version

def create_vernacular_version(
        collection_id: Union[int, str],
        suffix: Optional[str] = None
    ) -> str:
    """
    Given a collection id, ex: 3433, and version suffix, ex: v1, creates a new
    vernacular version, ex: 3433/vernacular_metadata_v1/

    If no suffix is provided, uses the current datetime.
    """
    version_path = f"{collection_id}"
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{version_path}/vernacular_metadata_{suffix}/"

def create_mapped_version(
        vernacular_version: str, suffix: Optional[str] = None) -> str:
    """
    Given a vernacular version, ex: 3433/vernacular_metadata_v1/ and version
    suffix, ex: v2, creates a new mapped version, ex:
    3433/vernacular_metadata_v1/mapped_metadata_v2/

    If no suffix is provided, uses the current datetime.
    """
    vernacular_version = vernacular_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{vernacular_version}/mapped_metadata_{suffix}/"

def create_validation_version(
        mapped_version: str,
        suffix: Optional[str] = None
):
    """
    Given a mapped version, ex: 3433/vernacular_metadata_v1/mapped_metadata_v2/
    and a version suffix, ex: v2, creates a new validation version, ex:
    3433/vernacular_metadata_v1/mapped_metadata_v2/validation_v2.csv
    Validation versions paths are also version pages.

    If no suffix is provided, uses the current datetime.
    """
    mapped_version = mapped_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{mapped_version}/validation_{suffix}.csv"

def create_with_content_urls_version(
        mapped_version: str, suffix: Optional[str] = None) -> str:
    """
    Given a mapped version, ex: 3433/vernacular_metadata_v1/mapped_metadata_v2/
    and a version suffix, ex: v2, creates a new with content urls version, ex:
    3433/vernacular_metadata_v1/mapped_metadata_v2/with_content_urls_v2/

    If no suffix is provided, uses the current datetime.
    """
    mapped_version = mapped_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{mapped_version}/with_content_urls_{suffix}/"

def create_merged_version(
        with_content_urls_version: str, suffix: Optional[str] = None) -> str:
    """
    Given a with content urls version, ex: 
    3433/vernacular_metadata_v1/mapped_metadata_v2/with_content_urls_v2/ and a
    version suffix, ex: v1, creates a new merged version, ex:
    3433/vernacular_metadata_v1/mapped_metadata_v2/with_content_urls_v2/merged_v1/

    If no suffix is provided, uses the current datetime.
    """
    with_content_urls_version = with_content_urls_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{with_content_urls_version}/merged_{suffix}/"

def get_most_recent_vernacular_version(collection_id: Union[int, str]):
    """
    Sorts the contents of $RIKOLTI_DATA/<collection_id>/, and returns the
    version path of the first item - this presumes a sortable vernacular
    version suffix.
    """
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    versions = storage.list_dirs(f"{data_root.rstrip('/')}/{collection_id}/")
    if not versions:
        raise Exception(
            "No vernacular metadata versions found for {collection_id}")
    recent_version = sorted(versions)[-1]
    return f"{collection_id}/{recent_version}/"

def get_most_recent_mapped_version(collection_id: Union[int, str]):
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    collection_path = f"{data_root.rstrip('/')}/{collection_id}/"
    vernacular_versions = storage.list_dirs(collection_path)
    if not vernacular_versions:
        raise Exception(
            "No vernacular metadata versions found for {collection_id}")
    vernacular_version = sorted(vernacular_versions)[-1]
    mapped_versions = storage.list_dirs(f"{collection_path}{vernacular_version}/")
    if not mapped_versions:
        raise Exception(
            "No mapped metadata versions found for {collection_id} at {vernacular_version}")
    recent_version = sorted(mapped_versions)[-1]
    return f"{collection_id}/{vernacular_version}/{recent_version}/"

def get_versioned_pages(version, **kwargs):
    """
    resolves a vernacular version to a data_uri at $RIKOLTI_DATA/<version>/
    returns a list of version pages.
    """
    if not version:
        raise ValueError("versions.get_versioned_pages: No version path provided")
    recursive = True
    if "recursive" in kwargs:
        recursive = kwargs.pop("recursive")
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    data_path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/"
    page_list = storage.list_pages(data_path, recursive=recursive, **kwargs)
    return [path[len(data_root)+1:] for path in page_list]

def get_child_directories(version, **kwargs):
    """
    resolves a mapped version to a data_uri at $RIKOLTI_DATA/<version>/data/
    returns a list of directories.

    complex objects are stored in a directory named "children" within the
    mapped version data directory. This function is used to check if any
    directory named "children" is inside the mapped version's data directory.
    """
    data_root = os.environ.get('RIKOLTI_DATA', "file:///tmp")
    child_directories = storage.list_dirs(
        f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/",
        recursive=False
    )
    return child_directories

def get_child_pages(version, **kwargs):
    """
    resolves a mapped version to a data_uri at $RIKOLTI_DATA/<version>/data/children/
    returns a list of version pages located at data_uri.
    """
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    data_path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/children/"
    try:
        page_list = storage.list_pages(data_path, recursive=False, **kwargs)
    except FileNotFoundError:
        return []
    except OSError:
        return []
    return [path[len(data_root)+1:] for path in page_list]

def get_versioned_page_content(version_page):
    """
    resolves a version page to a data_uri at $RIKOLTI_DATA/<version_page>/
    returns the contents of the page.
    """
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp").rstrip('/')
    content = storage.get_page_content(f"{data_root}/{version_page}")
    return content

def get_versioned_page_as_json(version_page):
    content = get_versioned_page_content(version_page)
    return json.loads(content)

def put_versioned_page(content: str, page_name: Union[int, str], version: str):
    """
    resolves a version path to a page uri at $RIKOLTI_DATA/<version>/data/<page_name>.jsonl
    and writes content to that data uri. returns the version page.

    content should be a json.dumped string of a list of dicts.
    """
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/{page_name}"
    storage.put_page_content(content, path)
    return f"{version.rstrip('/')}/data/{page_name}"

def put_validation_report(content, version_page):
    """
    resolves a version path to a page uri at $RIKOLTI_DATA/<version page>
    and writes content to that data uri. returns the version page.

    content should be a csv string.
    """
    data_root = os.environ.get("RIKOLTI_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version_page}"
    storage.put_page_content(content, path)
    return version_page
