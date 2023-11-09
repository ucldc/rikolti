import os
import re

import boto3
from datetime import datetime

from urllib.parse import urlparse
from typing import Optional
from collections import namedtuple

DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)


def list_dirs(data_uri: str, **kwargs) -> list[str]:
    data = parse_data_uri(data_uri)
    if data.store == 's3': 
        s3 = boto3.client('s3', **kwargs)
        s3_objects = s3.list_objects_v2(
            Bucket=data.bucket, 
            Prefix=data.path,
            Delimiter='/'
        )
        keys = [
            obj['Prefix'][len(data.path):-1] 
            for obj in s3_objects['CommonPrefixes']
        ]
        return keys
    elif data.store == 'file':
        dir_contents = os.listdir(data.path)
        dirs = [
            file for file in dir_contents
            if os.path.isdir(os.path.join(data.path, file))
        ]
        return dirs
    else:
        raise Exception(f"Unknown data store: {data.store}")


def list_pages(data_uri: str, recursive: bool=True, **kwargs) -> list:
    data = parse_data_uri(data_uri)

    if data.store == 's3':
        try:
            return list_s3_pages(data, recursive=recursive, **kwargs)
        except Exception as e:
            url = (
                f"https://{data.bucket}.s3.us-west-2.amazonaws"
                ".com/index.html#{data.path}/"
            )
            raise Exception(
                f"Error listing files at {data.uri}\n"
                f"Check that {data.path} exists at {url}\n{e}"
        )
    elif data.store == 'file':
        try:
            return list_file_pages(data, recursive=recursive)
        except Exception as e:
            raise Exception(f"Error listing files in {data.path}\n{e}")
    else:
        raise Exception(f"Unknown data store: {data.store}")


def list_s3_pages(data: DataStorage, recursive: bool=True, **kwargs) -> list:
    """
    List all objects in s3_bucket with prefix s3_prefix
    """
    s3 = boto3.client('s3', **kwargs)

    s3_objects = s3.list_objects_v2(
        Bucket=data.bucket, 
        Prefix=data.path
    )
    # TODO: check resp['IsTruncated'] and use ContinuationToken if needed

    keys = [f"s3://{data.bucket}/{obj['Key']}" for obj in s3_objects['Contents']]
    prefix = f"s3://{data.bucket}/{data.path}"

    if not recursive:
        # prune deeper branches
        leaf_regex = re.escape(prefix) + r"^\/?[\w!'_.*()-]+\/?$"
        keys = [key for key in keys if re.match(leaf_regex, key)]

    return keys


def list_file_pages(data: DataStorage, recursive: bool=True) -> list:
    """
    List all files in file_path
    """
    file_objects = []
    if recursive:
        for root, dirs, files in os.walk(data.path):
            root_uri = "file://{root}/" if root[-1] != '/' else "file://{root}"
            for file in files:
                file_objects.append(f"{root_uri}{file}")

    if not recursive:
        for file in os.listdir(data.path):
            if os.path.isfile(os.path.join(data.path, file)):
                root_uri = "file://{data.path}/" if data.path[-1] != '/' else "file://{data.path}"
                file_objects.append(f"{root_uri}{file}")

    return file_objects


def get_page_content(data_uri: str, **kwargs):
    data = parse_data_uri(data_uri)
    if data.store == 's3':
        return get_s3_contents(data)
    elif data.store == 'file':
        return get_file_contents(data)
    else:
        raise Exception(f"Unknown data store: {data.store}")


def get_s3_contents(data: DataStorage, **kwargs):
    """
    Get the body of the object located at data.path
    """
    s3 = boto3.client('s3', **kwargs)

    try:
        obj = s3.get_object(Bucket=data.bucket, Key=data.path)
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        url = (
            f"https://{data.bucket}.s3.us-west-2.amazonaws.com/"
            f"index.html#{data.path}/"
        )
        raise Exception(
            f"Error reading file at {data.uri}\nCheck: {url}\n{e}"
        )


def get_file_contents(data: DataStorage):
    """
    Get the body of the file located at file_path
    """
    try:
        with open(data.path, 'r') as f:
            return f.read()
    except Exception as e:
        raise Exception(f"Error reading {data.path}\n{e}")


def put_page_content(content:str, data_uri: str, **kwargs) -> str:
    """
    Write content to a file at relative_path (relative to data_path).
    relative_path is a list of strings, each string is a directory name 
    representing a directory tree.
    handle s3 or file storage, use '/' as separator for s3 key and os.sep
    as separtors for file storage
    """
    data = parse_data_uri(data_uri)

    if data.store == 's3':
        return put_s3_content(data, content, **kwargs)
    elif data.store == 'file':
        return put_file_content(data, content)
    else:
        raise Exception(f"Unknown data store: {data.store}")


def put_s3_content(data: DataStorage, content, **kwargs) -> str:
    """
    Write content to an object named data.path
    """
    s3 = boto3.client('s3', **kwargs)
    s3.put_object(
        ACL='bucket-owner-full-control',
        Bucket=data.bucket,
        Key=data.path,
        Body=content
    )
    return data.uri

def put_file_content(data: DataStorage, content) -> str:
    """
    Write content to a file at data.path
    """
    file_path = os.sep.join(data.path.split('/'))
    directory_path = os.path.dirname(file_path)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    with open(file_path, 'w') as f:
        f.write(content)
    return data.uri


def create_vernacular_version(
        collection_id: int or str,
        vernacular_suffix: Optional[str] = None
    ):
    fetcher_data_dest = os.environ.get(
        "FETCHER_DATA_DEST", "file:///tmp")
    vernacular_root = (
        f"{fetcher_data_dest.rstrip('/')}/{collection_id}/")
    if not vernacular_suffix:
        vernacular_suffix = (
            datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    vernacular_path = (
        f"{vernacular_root}vernacular_metadata_{vernacular_suffix}/")
    return vernacular_path


class RikoltiStorage():
    def __init__(
            self, 
            collection_id: int or str, 
            vernacular_suffix: Optional[str] = None,
            vernacular_path: Optional[str] = None,
            mapped_data_suffix: Optional[str] = None,
            mapped_data_path: Optional[str] = None,
            **kwargs):

        self.collection_id = collection_id

        if not vernacular_path:
            fetcher_data_dest = os.environ.get(
                "FETCHER_DATA_DEST", "file:///tmp")
            vernacular_root = (
                f"{fetcher_data_dest.rstrip('/')}/{collection_id}/")
            if not vernacular_suffix:
                vernacular_suffix = (
                    datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
            vernacular_path = (
                f"{vernacular_root}vernacular_metadata_{vernacular_suffix}/")

        self.vernacular = vernacular_path.rstrip('/')+"/"

        mapped_data_dest = os.environ.get("MAPPED_DATA_DEST", "file:///tmp")
        mapped_root = (
            f"{mapped_data_dest.rstrip('/')}/{self.collection_id}/"
        )
        

    def save_fetched_content(self, content: str, filename: str):
        return put_page_content(content, f"{self.vernacular}data/{filename}")

    # def list_fetched_content(self, recursive: bool=True, **kwargs) -> list:
    #     return list_pages(
    #         f"{self.vernacular_data}/{self.collection_id}/"
    #         f"vernacular_metadata{self.suffix}/",
    #         recursive=recursive
    #     )

    def search_page(self, search_str: str, page: str) -> bool:
        if self.data_store == 's3':
            return self.search_s3_page(search_str, page)
        elif self.data_store == 'file':
            return self.search_file_page(search_str, page)
        else:
            raise Exception(f"Unknown data store: {self.data_store}")

    def search_s3_page(self, search_str: str, s3_key: str) -> bool:
        """
        Check if search_str is in the body of the object located at s3_key
        Returns the s3_key of the object if so, otherwise returns None
        """
        obj = self.s3.get_object(Bucket=self.data_bucket, Key=s3_key)
        body = obj['Body'].read().decode('utf-8')
        if search_str in body:
            return True
        else:
            return False

    def search_file_page(self, search_str: str, file_path: str) -> bool:
        """
        Check if search_str is in the body of the file located at file_path
        """
        with open(file_path, 'r') as f:
            body = f.read()
            if search_str in body:
                return True
            else:
                return False




