import json
import os
from datetime import datetime
from typing import Union, Optional
from . import storage

def get_version(collection_id: Union[int, str], uri: str) -> str:
    """
    From an arbitrary path, try to get the version string
    """
    collection_id = str(collection_id)
    uri = uri.rstrip('/')
    if str(collection_id) not in uri or uri.endswith(str(collection_id)):
        raise Exception("Not a valid version path")
    rikolti_data_root, relative_path = uri.split(f"{collection_id}/")
    path_list = relative_path.split('/')
    if 'data' in path_list:
        path_list = path_list[:path_list.index('data')]
    path_list.insert(0, str(collection_id))
    version = "/".join(path_list)
    return version

def create_vernacular_version(
        collection_id: Union[int, str],
        suffix: Optional[str] = None
    ) -> str:
    version_path = f"{collection_id}"
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{version_path}/vernacular_metadata_{suffix}/"

def create_mapped_version(
        vernacular_version: str, suffix: Optional[str] = None) -> str:
    vernacular_version = vernacular_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{vernacular_version}/mapped_metadata_{suffix}/"

def create_validation_version(
        mapped_version: str,
        suffix: Optional[str] = None
):
    mapped_version = mapped_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{mapped_version}/validation_{suffix}.csv"

def create_content_data_version(
        mapped_version: str, suffix: Optional[str] = None) -> str:
    mapped_version = mapped_version.rstrip('/')
    if not suffix:
        suffix = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return f"{mapped_version}/content_data_{suffix}/"

def get_most_recent_vernacular_version(collection_id: Union[int, str]):
    data_root = os.environ.get("VERNACULAR_DATA", "file:///tmp")
    versions = storage.list_dirs(f"{data_root.rstrip('/')}/{collection_id}/")
    if not versions:
        raise Exception(
            "No vernacular metadata versions found for {collection_id}")
    recent_version = sorted(versions)[-1]
    return f"{collection_id}/{recent_version}/"

def get_vernacular_pages(version, **kwargs):
    data_root = os.environ.get('VERNACULAR_DATA', "file:///tmp")
    data_path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/"
    page_list = storage.list_pages(data_path, recursive=True, **kwargs)
    return [path[len(data_root)+1:] for path in page_list]

def get_mapped_pages(version, **kwargs):
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")
    data_path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/"
    page_list = storage.list_pages(data_path, recursive=True, **kwargs)
    return [path[len(data_root)+1:] for path in page_list]

def get_child_directories(version, **kwargs):
    data_root = os.environ.get('MAPPED_DATA', "file:///tmp")
    child_directories = storage.list_dirs(
        f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/",
        recursive=False
    )
    return child_directories

def get_child_pages(version, **kwargs):
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")
    data_path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/children/"
    try:
        page_list = storage.list_pages(data_path, recursive=False, **kwargs)
    except FileNotFoundError:
        return []
    except OSError:
        return []
    return [path[len(data_root)+1:] for path in page_list]

def get_vernacular_page_content(version_page):
    data_root = os.environ.get("VERNACULAR_DATA", "file:///tmp").rstrip('/')
    return storage.get_page_content(f"{data_root.rstrip('/')}/{version_page}")

def get_mapped_page_content(version_page):
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp").rstrip('/')
    content = storage.get_page_content(f"{data_root.rstrip('/')}/{version_page}")
    return json.loads(content)

def put_vernacular_page(content: str, page_name: Union[int, str], version: str):
    data_root = os.environ.get("VERNACULAR_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/{page_name}"
    storage.put_page_content(content, path)
    return f"{version.rstrip('/')}/data/{page_name}"

def put_mapped_page(content, page_name, version):
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/{page_name}.jsonl"
    storage.put_page_content(content, path)
    return f"{version.rstrip('/')}/data/{page_name}.jsonl"

def put_content_data_page(content, page_name, version):
    data_root = os.environ.get("CONTENT_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version.rstrip('/')}/data/{page_name}"
    storage.put_page_content(content, path)
    return f"{version.rstrip('/')}/data/{page_name}"

def put_validation_report(content, version_page):
    data_root = os.environ.get("MAPPED_DATA", "file:///tmp")
    path = f"{data_root.rstrip('/')}/{version_page}"
    storage.put_page_content(content, path)
    return version_page