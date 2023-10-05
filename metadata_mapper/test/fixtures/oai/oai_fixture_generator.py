from datetime import datetime
from random import randint
from typing import Any

from faker import Faker

from ..fixture_generator import FixtureGenerator

class OaiFixtureGenerator(FixtureGenerator):
  
  DEFAULT_SCHEMA = {
    "contributor": str,
    "creator": str,
    "date": [datetime] * randint(1, 9),
    "description": [str] * randint(1, 3),
    "extent": str,
    "format": [str] * randint(1, 2),
    "identifier": [str] * randint(1, 2),
    "provenance": str,
    "publisher": str,
    "relation": [str] * randint(1, 14),
    "rights": [str] * randint(1, 2),
    "spatial": [str] * randint(1, 2),
    "subject": str,
    "temporal": str,
    "title": str,
    "type": str
  }