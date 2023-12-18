import importlib
import os

from datetime import datetime
from faker import Faker
from random import randint
from typing import Any, Union


class BaseTestHelper:
    """
    Generates fake data for use in mapper unit tests.

    By default, this class uses DEFAULT_SCHEMA as its schema definition.
    Any subclass can define SCHEMA, and it will be merged into
    DEFAULT_SCHEMA to generate an appropriate schema for any given mapper.

    If a field requires special logic to generate, define a method named
    "generate_{field_name}" where {field_name} equals the key in SCHEMA
    that you want to generate the value for.
    """

    # SCHEMAs will be merged together in order to generate
    # the final fixture schema.
    SCHEMA = {}

    # Values generated for any field name in STATIC_ATTRS is cached so that it
    # can be reused. This is especially useful for identifier fields that need
    # to be referenced multiple times throughout fixture generation.
    STATIC_ATTRS = ["id", "identifier"]

    @classmethod
    def for_mapper(cls, module_parts: list[str]) -> type["BaseTestHelper"]:
        helper_path = None
        module_len = len(module_parts)
        while module_len and not helper_path:
            helper_path = (
                "metadata_mapper/test/helpers/"
                + "/".join(module_parts[:module_len]).replace("_mapper", "")
                + "_helper.py"
            )
            if not os.path.exists(helper_path):
                helper_path = None
                module_len = module_len - 1

        if helper_path:
            helper_module_parts = [
                p.replace("_mapper", "_helper") for p in module_parts
            ]
            helper_class_name = (
                "".join(
                    [
                        word.title()
                        for word in module_parts[-1].replace("_mapper", "").split("_")
                    ]
                )
                + "TestHelper"
            )
            helper_module = importlib.import_module(
                f".helpers.{'.'.join(helper_module_parts)}",
                package="rikolti.metadata_mapper.test",
            )
            return getattr(helper_module, helper_class_name)
        else:
            return None

    def __init__(self, request_mock):
        self.request_mock = request_mock
        self.faker = Faker()
        self.collection_id = self.faker.pyint()
        self.static = {}
        self.setup_mocks()

    def setup_mocks(self):
        pass

    def instantiate_record(self, record_class) -> type["Record"]:
        fixture = self.generate_fixture()
        instance = record_class(self.collection_id, fixture)
        self.prepare_record(instance)
        return instance

    def prepare_record(self, record) -> None:
        record.legacy_couch_db_id = "asdf--123123"

    def generate_fixture(self, schema_index: int = 0) -> dict[str, Any]:
        """
        Generates a test data fixture.
        """
        schema = self.SCHEMA or {}

        superschemas = [
            super(c, self).SCHEMA
            for c in list(reversed(type(self).__mro__))
            if hasattr(super(c, self), "SCHEMA")
        ]



        for superschema in superschemas:
            schema = {**superschema, **schema}

        return {
            key: self.generate_value_for(key, type) for (key, type) in schema.items()
        }

    def generate_value_for(
        self,
        field_name: str = None,
        expected_type: Union[type, list, str] = str,
        skip_static: bool = False,
    ) -> Any:
        if isinstance(expected_type, str):
            return getattr(self, expected_type)()
        elif not skip_static and field_name in self.STATIC_ATTRS:
            if not self.static.get(field_name):
                self.static[field_name] = self.generate_value_for(
                    field_name, expected_type, skip_static=True
                )
                return self.static[field_name]
        elif isinstance(expected_type, type):
            return self.generate_value_of_type(expected_type)
        elif isinstance(expected_type, list):
            return [self.generate_value_for(item) for item in expected_type]

    def generate_value_of_type(self, type: type) -> Any:
        if type == str:
            return self.faker.pystr()
        elif type == datetime:
            return self.faker.date()

    # Helper methods

    def splittable_string(self) -> str:
        """Generate a string with semicolons to be split on"""
        return ";".join([self.faker.pystr() for _ in range(0, randint(1, 3))])

    def list_of_splittable_strings(self) -> list[str]:
        """
        Generate content to be split and flattened by mapper#split_and_flatten.
        """
        return [self.splittable_string() for _ in range(0, randint(1, 3))]
