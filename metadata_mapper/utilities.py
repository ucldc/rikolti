import importlib
import json
import os
from typing import Callable, Union

import boto3

from . import settings


def returns_callable(func: Callable) -> Callable:
    """
    A decorator that returns a lambda that calls the wrapped function when invoked
    """
    def inner(*args, **kwargs):
        return lambda: func(*args, **kwargs)

    return inner


def import_vernacular_reader(mapper_type):
    """
    accept underscored_module_name_prefixes
    accept CamelCase class name prefixes split on underscores
    for example:
    mapper_type | mapper module       | class name
    ------------|---------------------|------------------
    nuxeo       | nuxeo_mapper        | NuxeoVernacular
    content_dm  | content_dm_mapper   | ContentDmVernacular
    """
    from .mappers.mapper import Vernacular
    *mapper_parent_modules, snake_cased_mapper_name = mapper_type.split(".")

    mapper_module = importlib.import_module(
        f".mappers.{'.'.join(mapper_parent_modules)}.{snake_cased_mapper_name}_mapper",
        package=__package__
    )

    mapper_type_words = snake_cased_mapper_name.split('_')
    class_type = ''.join([word.capitalize() for word in mapper_type_words])
    vernacular_class = getattr(
        mapper_module, f"{class_type}Vernacular")

    if not issubclass(vernacular_class, Vernacular):
        print(f"{mapper_type} not a subclass of Vernacular")
        exit()
    return vernacular_class


def get_files(directory: str, collection_id: int) -> list[str]:
    """
    Gets a list of filenames in a given directory.
    """
    if settings.DATA_SRC["STORE"] == "file":
        path = settings.local_path(directory, collection_id)
        return [f for f in os.listdir(path)
                if os.path.isfile(os.path.join(path, f))]
    elif settings.DATA_SRC["STORE"] == "s3":
        s3_client = boto3.client('s3')
        resp = s3_client.list_objects_v2(
            Bucket=settings.DATA_SRC["BUCKET"],
            Prefix=f"{directory}/{collection_id}"
        )
        # TODO: check resp['IsTruncated'] and use ContinuationToken if needed
        return [page['Key'] for page in resp['Contents']]

def read_from_bucket(directory: str, collection_id: int,
                     file_name: Union[str, int]) -> str:
    """
    Reads the contents of a file from the appropriate content bucket.

    Data comes from local filesystem or S3, depending on ENV vars.

    Parameters:
        directory: str
        collection_id: str
            Files are separated into directories by collection_id
        file_name: Union[str, int]
            The name of the file to read

    Returns: str
        The file contents
    """
    if settings.DATA_SRC["STORE"] == 'file':
        page_path = os.sep.join([
            settings.local_path(directory, collection_id),
            str(file_name)
        ])

        with open(page_path, "r") as metadata_file:
            return metadata_file.read()
    elif settings.DATA_SRC["STORE"] == 's3':
        s3_client = boto3.client('s3')
        s3_obj_summary = s3_client.get_object(
            Bucket=settings.DATA_SRC["BUCKET"],
            Key=f"{directory}/{collection_id}/{file_name}"
        )
        return s3_obj_summary['Body'].read()


def read_mapped_metadata(collection_id: int, page_id: int) -> list[dict]:
    """
    Reads and parses the content of a mapped metadata file.

    Parameters:
        collection_id: int
            The collection ID
        page_id: int
            The page ID (filename) to read and parse

    Returns: list[dict]
        The parsed data
    """
    return json.loads(read_from_bucket("mapped_metadata", collection_id, page_id))


def read_vernacular_metadata(collection_id: int, page_id: int) -> list[dict]:
    """
    Reads and parses the content of a vernacular (unmapped) metadata file.

    Parameters:
        collection_id: int
            The collection ID
        page_id: int
            The page ID (filename) to read and parse

    Returns: list[dict]
        The parsed data
    """
    return json.loads(read_from_bucket("vernacular_metadata", collection_id, page_id))


def write_to_bucket(directory: str, collection_id: int,
                    file_name: Union[str, int], content: str,
                    append: bool = False) -> None:
    if isinstance(content, list) or isinstance(content, dict):
        content = json.dumps(content)

    if settings.DATA_SRC["STORE"] == 'file':
        dir_path = settings.local_path(directory, collection_id)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        page_path = os.sep.join([dir_path, str(file_name)])

        with open(page_path, "a" if append else "w") as file:
            file.write(content)
    elif settings.DATA_SRC["STORE"] == 's3':
        s3_client = boto3.client('s3')
        key = (
            f"{directory}/{collection_id}/"
            f"{file_name}"
        )
        s3_client.put_object(
            Bucket=settings.DATA_DEST["BUCKET"],
            Key=key,
            Body=content)
