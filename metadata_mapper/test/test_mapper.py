import importlib
import os
import pytest
import re

from typing import Type

from unittest import mock

from .helpers.test_helper import TestHelper

class TestMapper:

   DEFAULT_TEST_METHOD_NAME = "generic_mapper"

   def find_mappers_to_test(self, start_path = "metadata_mapper/mappers"):
      ret = {}

      for dir in os.scandir(start_path):
         if dir.is_dir():
            ret = { **ret, **self.find_mappers_to_test(dir.path) }
         elif dir.is_file() and dir.name.endswith("_mapper.py"):
            path_regex_result = re.search("([\w\/]+?_mapper).py", dir.path)
            if path_regex_result:
               mapper_path = path_regex_result[1].replace("/", ".").lstrip(".")
               mapper_name = mapper_path.split(".")[-1]

               if f"test_{mapper_name}" in locals().keys():
                  ret[mapper_path] = getattr(self, f"test_{mapper_name}")
               else:
                  ret[mapper_path] = getattr(self, self.DEFAULT_TEST_METHOD_NAME)

      return ret

   def get_helper(self, mapper_path) -> type["TestHelper"]:
      module_parts = mapper_path.replace("_mapper", "").split(".")
      helper_path = f"./helpers/{'/'.join(module_parts)}_helper.py"
      if os.path.exists(helper_path):
         helper_module_name = helper_path.replace("/", ".").rstrip(".py")
         helper_class_name = f"{self.camelize(module_parts[-1])}TestHelper"
         helper_module = importlib.import_module(helper_module_name)
         return getattr(helper_module, helper_class_name)
      else:
         return TestHelper

   def get_record(self, mapper_path, module) -> type["Mapper"]:
      mapper_name = mapper_path.replace("_mapper", "").split(".")[-1]
      class_name = f"{self.camelize(mapper_name)}Record"
      return getattr(module, class_name)

   def camelize(self, words: str) -> str:
      return "".join([word.title() for word in words.split("_")])

   def generic_mapper(self, record_class, test_data):
      pass

   # Test methods (invoked by pytest)

   # This will loop through all mappers that don't have explicit test methods and
   # run them with default data
   def test_mappers(self):
      default_test_method = getattr(self, self.DEFAULT_TEST_METHOD_NAME)
      
      mappers = [mapper for mapper, method in self.find_mappers_to_test().items()
                 if method == default_test_method]

      for mapper in mappers:
         module = importlib.import_module(mapper)
         helper = self.get_helper(mapper)()
         record_class = self.get_record(mapper, module)
         test_data = helper.generate_fixture()

         default_test_method(record_class, test_data)

         
