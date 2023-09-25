from datetime import datetime
from typing import Any

from faker import Faker

class FixtureGenerator:
  
  FIXTURE_FIELDS = {
    "id": str,
    "identifier": str,
    "contributor": str,
    "availabe": datetime,
    "created": datetime,
    "date": datetime,
    "dateAccepted": datetime,
    "dateCopyrighted": datetime,
    "dateSubmitted": datetime,
    "issued": datetime,
    "modified": datetime,
    "valid": datetime,
    "description": str,
    "abstract": str,
    "tableOfContents": str,
    "extent": str,
    "format": str,
    "medium": str,
    "bibliographicCitation": str,
    "provenance": str,
    "publisher": str,
    "conformsTo": str,
    "hasFormat": str,
    "hasPart": str,
    "hasVersion": str,
    "isFormatOf": str,
    "isPartOf": str,
    "isReferencedBy": str,
    "isReplacedBy": str,
    "isRequiredBy": str,
    "isVersionOf": str,
    "references": str,
    "relation": str,
    "replaces": str,
    "require": str,
    "accessRights": str,
    "rights": str,
    "coverage": str,
    "spatial": str,
    "subject": str,
    "temporal": str,
    "title": str,
    "type": str
  }

  STATIC_ATTRS = [
    "id"
    "identifier"
  ]

  def __init__(self):
    self.faker = Faker()
    self.static = []

  def generate(self) -> dict[str, Any]:
    return {key: self.generate_value_for(key, type) 
            for (key, type) 
            in self.FIXTURE_FIELDS.items()
            }

  def generate_value_for(self, field_name: str = None, 
                         type: type = str, skip_static: bool = False) -> Any:
    if hasattr(self, f"generate_{field_name}"):
      return getattr(self, f"generate_{field_name}")()
    elif not skip_static and field_name in self.STATIC_ATTRS:
      if not self.static[field_name]:
        self.static[field_name] = self.generate_value_for(field_name, type, 
                                                          skip_static=True)
        return self.static[field_name]
    else:
      if type == str:
        return self.faker.pystr()
      elif type == datetime:
        return self.faker.date()