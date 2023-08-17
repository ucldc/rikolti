import re
from typing import Any

from ..mapper import ValidationLogLevel, Validator
from .flickr_mapper import FlickrRecord, FlickrValidator, FlickrVernacular


class SpplRecord(FlickrRecord):
    def UCLDC_map(self):
        return {
            "description": [self.map_description()],
            "identifier": self.map_identifier(),
            "type": self.search_description(r"Type:([^\n]+)\n\s*\n"),
            "rights": [self.search_description(r"Rights Information:([\S\s]+)")],
            "provenance": [self.search_description(r"Source:([^\n]+)\n\s*\n")],
            "subject": self.map_subject(),
            "date": self.search_description(r"Date:([^\n]+)\n\s*\n")
        }

    @property
    def source_description(self):
        return self.source_metadata.get("description", {}).get("_content")

    def map_description(self):
        description = self.source_description
        description = re.sub(r"Type:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(r"Source:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(r"Date:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(r"Identifier:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(
            r"Local Call number:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(
            r"Previous Identifier:(.+\n?(?!\n).*)\n\s*\n", "", description, count=1)
        description = re.sub(r"Category:([^\n]+)\n\s*\n", "", description, count=1)
        description = re.sub(r"Rights Information:([\S\s]+)", "", description, count=1)
        return description

    def search_description(self, regex):
        matches = re.search(regex, self.source_description, re.MULTILINE)
        if matches:
            return matches.groups()[-1].strip()

    def map_subject(self):
        subjects = [{"name": tag.get("raw")} for tag
                    in self.source_metadata.get("tags", {}).get("tag", [])]

        category = self.search_description(r"Category:([^\n]+)\n\s*\n")

        if category:
            subjects.append({"name": category.lower()})

        return subjects

    def map_identifier(self):
        """
        Combine `previous_identifier` and `identifier` values from description
        metadata. 
        """
        previous_identifiers = self.search_description(
            r"Previous Identifier:(.+\n?(?!\n).*)\n\s*\n") or []
        if previous_identifiers:
            previous_identifiers = previous_identifiers.replace("N/A", "")
            previous_identifiers = re.split(r"\s+/\s+", previous_identifiers)
        
        local_call_number = self.search_description(
            r"Local Call number:([^\n]+)\n\s*\n"
        )
        if local_call_number:
            previous_identifiers.append(local_call_number)

        identifiers = self.search_description(r"Identifier:([^\n]+)\n\s*\n")
        if identifiers:
            previous_identifiers.append(identifiers)

        return [i for i in previous_identifiers if i != "N/A"]


class SpplFlickrValidator(FlickrValidator):
    def __init__(self, **options):
        super().__init__(**options)
        self.add_validatable_field(
            field="description", type=str,
            validations=[
                SpplFlickrValidator.content_match,
            ],
            level=ValidationLogLevel.WARNING
        )

    @staticmethod
    def content_match(validation_def: dict, rikolti_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is equal.

        If content_match validation fails, check if comparison_value is None
        and rikolti_value is ['Owner: South Pasadena Public Library']
        """
        content_match_validation = Validator.content_match(
            validation_def, rikolti_value, comparison_value)

        if content_match_validation == "Content mismatch":
            if (comparison_value is None and
                 rikolti_value == ['Owner: South Pasadena Public Library']):
                return
        return content_match_validation


class SpplVernacular(FlickrVernacular):
    record_cls = SpplRecord
    validator = SpplFlickrValidator

