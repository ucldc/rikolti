import importlib
import json
from typing import Callable, Union

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

