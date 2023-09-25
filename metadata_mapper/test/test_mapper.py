import os
import pytest

from fixture_generator import FixtureGenerator

from metadata_mapper.mappers.oai.islandora_mapper import IslandoraRecord

@pytest.fixture
def test_data(generator_class = FixtureGenerator):
    generator = generator_class()
    data = generator.generate()
    yield data

def test_islandora_mapper(test_data):
    r = IslandoraRecord(test_data["collection_id"], test_data)
    r.to_UCLDC()

# This will loop through all mappers that don't have explicit test methods and
# run them with default data
def test_other_mappers(test_data):
  for dir in os.scandir("../mappers"):
     if dir.is_dir():
        for file in os.scandir(dir.path):
           if file.name.endswith("_mapper.py"):
              name = file.name.replace("_mapper.py", "")
              if f"test_{name}_mapper" in list(locals().keys):
                 continue

              package = locals().get(f"{name}_mapper")
              if not package:
                continue

              camelized_name = ''.join(name.capitalize() or "_" for x in name.split("_"))
              record_class = package.getattr(f"{camelized_name}Record")
              r = record_class(test_data["collection_id"], test_data)
              r.to_UCLDC()