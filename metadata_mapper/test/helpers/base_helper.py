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

    def __init__(self, request_mock=None):
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
        schema = self.merged_schema(schema_index)

        return {
            key: self.generate_value_for(key, type) for (key, type) in schema.items()
        }

    def merged_schema(self, schema_index: int = 0) -> dict[str, Any]:
        inheritance_chain = list(reversed(type(self).__mro__))
        superschemas = [
            super(cls, self).SCHEMA
            for cls in inheritance_chain
            if hasattr(super(cls, self), "SCHEMA")
        ]

        schema = {}
        for superschema in superschemas:
            schema = {**schema, **superschema}

        return {**schema, **self.SCHEMA}

    def generate_value_for(
        self,
        field_name: str = None,
        expected_type: Union[type, list, str] = str,
    ) -> Any:
        if isinstance(expected_type, str):
            return getattr(self, expected_type)()
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
