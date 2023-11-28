import os
import re

import boto3
import shutil

from urllib.parse import urlparse
from collections import namedtuple


"""
This module implements list, get, and put operations for s3:// or file:// URIs.
Broadly, these functions take a data_uri, where a data_uri is always a
URI-formatted absolute path to a storage location, and kwargs, which are always
passed along to the underlying boto3 call and can be used for AWS credentials
"""


DataStorage = namedtuple(
    "DateStorage", "uri, store, bucket, path"
)

def parse_data_uri(data_uri: str):
    data_loc = urlparse(data_uri)
    return DataStorage(
        data_uri, data_loc.scheme, data_loc.netloc, data_loc.path)


def list_dirs(data_uri: str, recursive=False, **kwargs) -> list[str]:
    """
    Returns a list of all directories that are the immediate child of data_uri

    Returns relative paths - it's the only function in this module that does
    not return absolute paths, and it might be worth changing it for
    consistency, even though it's usage does not require absolute paths.

    There is no recursive implementation at this time.
    """
    data = parse_data_uri(data_uri)
    if data.store == 's3': 
        s3 = boto3.client('s3', **kwargs)
        s3_objects = s3.list_objects_v2(
            Bucket=data.bucket, 
            Prefix=data.path.lstrip('/'),
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
    """
    Returns a list of all files that are the child of data_uri as URI-formatted
    absolute paths.

    Recursively traverses a directory tree by default, pass recursive=False to
    return only direct children.
    """
    data = parse_data_uri(data_uri)

    if data.store == 's3':
        try:
            return list_s3_pages(data, recursive=recursive, **kwargs)
        except Exception as e:
            url = (
                f"https://{data.bucket}.s3.us-west-2.amazonaws"
                ".com/index.html#{data.path}/"
            )
            print(
                f"Error listing files at {data.uri}\n"
                f"Check that {data.path} exists at {url}\n{e}"
            )
            raise FileNotFoundError
    elif data.store == 'file':
        try:
            return list_file_pages(data, recursive=recursive)
        except Exception as e:
            print(f"Error listing files in {data.path}\n{e}")
            raise FileNotFoundError
    else:
        raise Exception(f"Unknown data store: {data.store}")


def list_s3_pages(data: DataStorage, recursive: bool=True, **kwargs) -> list:
    """
    List all objects in s3_bucket with prefix s3_prefix
    """
    s3 = boto3.client('s3', **kwargs)
    s3_objects = s3.list_objects_v2(
        Bucket=data.bucket, 
        Prefix=data.path.lstrip('/')
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
            root_uri = f"file://{root}/" if root[-1] != '/' else f"file://{root}"
            for file in files:
                file_objects.append(f"{root_uri}{file}")

    if not recursive:
        for file in os.listdir(data.path):
            if os.path.isfile(os.path.join(data.path, file)):
                root_uri = f"file://{data.path}/" if data.path[-1] != '/' else f"file://{data.path}"
                file_objects.append(f"{root_uri}{file}")

    return file_objects


def get_page_content(data_uri: str, **kwargs):
    """
    Returns the contents of the file stored at data_uri.
    """
    data = parse_data_uri(data_uri)
    if data.store == 's3':
        return get_s3_contents(data, **kwargs)
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
        obj = s3.get_object(Bucket=data.bucket, Key=data.path.lstrip('/'))
        return obj['Body'].read().decode('utf-8')
    except Exception as e:
        url = (
            f"https://{data.bucket}.s3.us-west-2.amazonaws.com/"
            f"index.html#{data.path}/"
        )
        raise FileNotFoundError(
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
        raise FileNotFoundError(f"Error reading {data.path}\n{e}")


def put_page_content(content:str, data_uri: str, **kwargs) -> str:
    """
    Writes content to a file located at data_uri and returns data_uri.
    Creates any subdirectories required to successfully write to data_uri.
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
        Key=data.path.lstrip('/'),
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
        os.makedirs(directory_path, exist_ok=True)

    with open(file_path, 'w') as f:
        f.write(content)
    return data.uri


def upload_file(filepath:str, data_uri: str, **kwargs):
    """
    Moves the file located at filepath to data_uri, returns data_uri.
    Creates any subdirectories required to successfully write to data_uri.
    """
    data = parse_data_uri(data_uri)

    if data.store == 's3':
        return upload_s3_file(data, filepath, **kwargs)
    elif data.store == 'file':
        return move_file(data, filepath)
    else:
        raise Exception(f"Unknown data store: {data.store}")

def upload_s3_file(data: DataStorage, filepath, **kwargs):
    """
    Upload a file to s3 at data.path
    """
    s3 = boto3.client('s3', **kwargs)
    s3.upload_file(
        filepath,
        data.bucket,
        data.path.lstrip('/')
    )
    return data.uri

def move_file(data: DataStorage, filepath):
    destination_path = os.sep.join(data.path.split('/'))
    directory_path = os.path.dirname(destination_path)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)

    shutil.copyfile(filepath, destination_path)
    return data.uri
