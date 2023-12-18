from datetime import datetime
from random import randint

from ..base_helper import BaseTestHelper

class OaiTestHelper(BaseTestHelper):
  
  DEFAULT_SCHEMA = {
    "contributor": str,
    "creator": str,
    "date": [datetime] * randint(1, 9),
    "description": [str] * randint(1, 3),
    "extent": str,
    "format": [str] * randint(1, 2),
    "id": str,
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
  
  def prepare_record(self, record) -> None:
    record.select_id(["id"])