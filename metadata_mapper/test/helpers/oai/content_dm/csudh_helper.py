from datetime import datetime
from random import randint
from typing import Any

from .contentdm_helper import ContentdmTestHelper


class CsudhTestHelper(ContentdmTestHelper):
    SCHEMA = {
        "bibliographicCitation": str,
        "title": "csudh_title",
    }

    def csudh_title(self):
        return [f"csudh-{self.faker.pystr()}", self.faker.pystr()]
