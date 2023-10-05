from datetime import datetime
from random import randint
from typing import Any

from .oai_fixture_generator import OaiFixtureGenerator

class ContentdmFixtureGenerator(OaiFixtureGenerator):
  
  SCHEMA = {
    "contributor": "list_of_splittable_strings",
    "coverage": "splittable_string",
    "creator": "list_of_splittable_strings",
    "spatial": "splittable_string",
    "type": "list_of_splittable_strings",
    "language": "list_of_splittable_strings",
    "subject": "list_of_splittable_strings"
  }