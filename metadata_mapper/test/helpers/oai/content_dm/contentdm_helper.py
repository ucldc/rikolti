import re

from ..oai_helper import OaiTestHelper
from .....mappers.oai.content_dm.contentdm_mapper import ContentdmRecord


class ContentdmTestHelper(OaiTestHelper):
    SCHEMA = {
        "contributor": "list_of_splittable_strings",
        "coverage": "splittable_string",
        "creator": "list_of_splittable_strings",
        "identifier": "contentdm_identifier",
        "language": "list_of_splittable_strings",
        "spatial": "splittable_string",
        "subject": "list_of_splittable_strings",
        "type": "list_of_splittable_strings",
    }

    def setup_mocks(self):
        matcher = re.compile('utils/ajaxhelper')
        response = {
          "imageinfo": {
              "height": self.faker.pyint(),
              "width": self.faker.pyint()
          }
        }
        self.request_mock.register_uri('GET', matcher, json=response)

    def contentdm_identifier(self):
        # See ContentdmMapper#get_identifier_parts for required formated
        value = f"http://{self.faker.domain_name()}/" + \
                f"{ContentdmRecord.identifier_match}/" + \
                (f"{self.faker.pystr()}/") + \
                f"{self.collection_id}/" + \
                f"{self.faker.pystr()}/" + \
                str(self.faker.pyint())
        
        return [value]
