import importlib
import json
from typing import Callable, Union

from . import settings
from rikolti.utils.rikolti_storage import RikoltiStorage


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


def get_files(collection_id: int, directory: str) -> list[str]:
    """
    Gets a list of filenames in a given directory.
    """
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_SRC_URL}/{collection_id}/{directory}")
    return rikolti_data.list_pages(recursive=False, relative=True)


def read_from_bucket(collection_id: int, directory: str,
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
    rikolti_data = RikoltiStorage(
        f"{settings.DATA_SRC_URL}/{collection_id}/{directory}/{file_name}")
    return rikolti_data.get_page_content()
    

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
    return json.loads(read_from_bucket(collection_id, "mapped_metadata", page_id))


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
    return json.loads(read_from_bucket(collection_id, "vernacular_metadata", page_id))


def write_to_bucket(collection_id: int, directory: str,
                    file_name: Union[str, int], content: str) -> None:
    if isinstance(content, list) or isinstance(content, dict):
        content = json.dumps(content)

    rikolti_data = RikoltiStorage(f"{settings.DATA_SRC_URL}/{collection_id}/{directory}/")
    rikolti_data.put_page_content(content, str(file_name))
    file_location = f"{settings.DATA_SRC_URL}/{collection_id}/{directory}/{file_name}"

    return file_location
