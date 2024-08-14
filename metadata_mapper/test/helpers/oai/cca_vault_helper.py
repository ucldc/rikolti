from .oai_helper import OaiTestHelper

class CcaVaultTestHelper(OaiTestHelper):
  
  SCHEMA = {
    "language": "list_of_splittable_strings",
    "source": [str]
  }