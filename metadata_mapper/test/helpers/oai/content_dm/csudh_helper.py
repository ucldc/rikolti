from datetime import datetime
from random import randint
from typing import Any

from .contentdm_helper import ContentdmTestHelper

class CsudhTestHelper(ContentdmTestHelper):
  
  SCHEMA = {
    "contributor": "list_of_splittable_strings",
    "coverage": "splittable_string",
    "creator": "list_of_splittable_strings",
    "spatial": "splittable_string",
    "type": "list_of_splittable_strings",
    "language": "list_of_splittable_strings",
    "subject": "list_of_splittable_strings"
  }