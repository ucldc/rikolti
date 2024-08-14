import importlib
import os
import pytest
import re
import requests_mock
import traceback

from textwrap import dedent

from .helpers.base_helper import BaseTestHelper
from ..mappers.mapper import Record


class TestMapper:
    DEFAULT_TEST_METHOD_NAME = "_test_generic_mapper"

    def find_mappers_to_test(self, start_path="metadata_mapper/mappers"):
        ret = {}

        for dir in os.scandir(start_path):
            if dir.is_dir():
                ret = {**ret, **self.find_mappers_to_test(dir.path)}
            elif dir.is_file() and dir.name.endswith("_mapper.py"):
                path_regex_result = re.search("([\\w\\/]+?_mapper).py", dir.path)
                if path_regex_result:
                    full_mapper_path = (
                        path_regex_result[1].replace("/", ".").lstrip(".")
                    )
                    module_parts = [
                        path
                        for path in full_mapper_path.split(".")
                        if path not in ["metadata_mapper", "mappers"]
                    ]
                    mapper_name = module_parts[-1]
                    mapper_path = ".".join(module_parts)

                    if f"test_{mapper_name}" in locals().keys():
                        ret[mapper_path] = getattr(self, f"test_{mapper_name}")
                    else:
                        ret[mapper_path] = getattr(self, self.DEFAULT_TEST_METHOD_NAME)

        return ret

    def get_record(self, module_parts, module) -> Record:
        mapper_name = module_parts[-1].replace("_mapper", "")
        class_name = f"{self.camelize(mapper_name)}Record"
        return getattr(module, class_name)

    def camelize(self, words: str) -> str:
        return "".join([word.title() for word in words.split("_")])

    def _test_generic_mapper(self, record_class, helper):
        try:
            instance = helper.instantiate_record(record_class)
            try:
                instance.to_UCLDC()
            except Exception as exc:
                pytest.assume(
                    False,
                    dedent(f"""\n**{type(instance).__name__}** raised error '{exc}'
                    at time of mapping.\n Here's the backtrace:\n
                    {traceback.format_exc()}"""),
                )
        except Exception as exc:
            pytest.assume(
                False,
                dedent(f"""\n**{record_class.__name__}** raised error '{exc}'
                at time of initialization.\n Here's the backtrace:\n
                {traceback.format_exc()}"""),
            )

    # Test methods (invoked by pytest)

    # This will loop through all mappers that don't have explicit test methods and
    # run them with default data
    def test_mappers(self, pytestconfig):
        with requests_mock.Mocker() as r_mock:
            default_test_method = getattr(self, self.DEFAULT_TEST_METHOD_NAME)

            mapper_filter = [
                mapper
                for mapper in re.split(
                    r"[,;]",
                    pytestconfig.getoption("mappers")
                    or pytestconfig.getoption("mapper")
                    or "",
                )
                if mapper
            ] or None

            mappers = [
                mapper
                for mapper, method in self.find_mappers_to_test().items()
                if method == default_test_method
            ]

            for mapper in mappers:
                module_parts = mapper.split(".")
                if (
                    mapper_filter
                    and module_parts[-1] not in mapper_filter
                    and module_parts[-1].replace("_mapper", "") not in mapper_filter
                ):
                    continue

                module = importlib.import_module(
                    f".mappers.{'.'.join(module_parts)}",
                    package="rikolti.metadata_mapper",
                )
                helper_class = BaseTestHelper.for_mapper(module_parts)

                if helper_class:
                    helper = helper_class(r_mock)
                    record_class = self.get_record(module_parts, module)
                    default_test_method(record_class, helper)
