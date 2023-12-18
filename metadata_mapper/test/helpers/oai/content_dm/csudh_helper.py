from datetime import datetime
from random import randint
from typing import Any

from .contentdm_helper import ContentdmTestHelper


class CsudhTestHelper(ContentdmTestHelper):
    SCHEMA = {
        "contributor": "list_of_splittable_strings",
        "coverage": "splittable_string",
        "creator": "list_of_splittable_strings",
        "identifier": "contentdm_identifier",
        "language": "list_of_splittable_strings",
        "spatial": "splittable_string",
        "subject": "list_of_splittable_strings",
        "type": "list_of_splittable_strings",
        "bibliographicCitation": str,
        "title": "csudh_title",
    }

    def csudh_title(self):
        return [f"csudh-{self.faker.pystr()}", self.faker.pystr()]
