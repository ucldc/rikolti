import os
import pytest

from unittest import mock

from .fixtures.fixture_generator import FixtureGenerator

@pytest.fixture
def test_data(generator_class = FixtureGenerator):
    generator = generator_class()
    data = generator.generate()
    yield data

def test_contentdm_mapper(test_data):
    from ..mappers.oai.content_dm.contentdm_mapper import ContentdmRecord

    r = ContentdmRecord(test_data.get("collection_id") or 123, test_data)
    r.legacy_couch_db_id = "asdf--asdf"
    r.to_UCLDC()

# This will loop through all mappers that don't have explicit test methods and
# run them with default data
def xtest_other_mappers(test_data):
  for dir in os.scandir("../mappers"):
     if dir.is_dir():
        for file in os.scandir(dir.path):
           if file.name.endswith("_mapper.py"):
              name = file.name.replace("_mapper.py", "")
              if f"test_{name}_mapper" in list(locals().keys()):
                 print(f"Running mapper-specific test for {name}")
                 continue

              package = locals().get(f"{name}_mapper")
              breakpoint()
              if not package:
                print(f"Mapper package not found for {name}")
                continue

              camelized_name = ''.join(name.capitalize() or "_" for x in name.split("_"))
              record_class = package.getattr(f"{camelized_name}Record")
              r = record_class(test_data["collection_id"], test_data)
              print(f"OMG WE DID IT {name}")
              r.to_UCLDC()