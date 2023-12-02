import re
from typing import Any

from .flickr_mapper import FlickrRecord, FlickrValidator, FlickrVernacular
from ..mapper import Validator


class SdasmRecord(FlickrRecord):
    def UCLDC_map(self):
        split_description = self.split_description()

        return {
            "description": [self.map_description(split_description)],
            "identifier": list(set(filter(None, [
                split_description.get("piction_id"),
                split_description.get("catalog"),
                split_description.get("filename"),
                split_description.get("sdasm_catalog"),
                split_description.get("catalog_or_negative_number")
            ]))),
            "date": list(filter(None, [
                split_description.get("date"),
                split_description.get("date_on_neg"),
                split_description.get("year")
            ]))
        }

    @property
    def source_description(self):
        return self.source_metadata.get("description", {}).get("_content", "")

    @staticmethod
    def get_mapping_configuration():
        """
        There are two formats that SDASM uses. One separates values with " - ".
        The other separates with linebreaks. The fields that use these two
        formats don't appear to mix fields, which is why this works. It might be
        possible to match both formats, but doesn't seem necessary at this time.
        """
        return [
            {
                "key": "piction_id",
                "regex": r"(^| )PictionID: ?(\S+)",
                "prepend": "PictionID: "
            },
            {
                "key": "catalog",
                "regex": r"(^| )Catalog: ?(\S+)",
                "prepend": "Catalog: "
            },
            {
                "key": "filename",
                "regex": r"(^| )Filename: ?(\S+)",
                "prepend": "Filename: "
            },
            {
                "key": "date_on_neg",
                "regex": r"(^| )Date on Neg: ?(\S+)",
                "keep_in_description": True
            },
            {
                "key": "year",
                "regex": r"(^| )Year: ?([^\n]+)\n",
                "keep_in_description": True
            },
            {
                "key": "date",
                "regex": r"(^| )Date: ?(\S+)",
                "keep_in_description": True
            },
            {
                "key": "sdasm_catalog",
                "regex": r"^SDASM Catalog #: ?([^\n]+)\n",
                "prepend": "SDASM Catalog #: "
            },
            {
                "key": "corp_name",
                "regex": r"^Corp. Name: ?([^\n]+)\n",
                "discard": True,
                "keep_in_description": True
            },
            {
                "key": "catalog_or_negative_number",
                "regex": r"^Catalog or Negative #: ?([^\n]+)\n",
                "prepend": "Catalog or Negative #: "
            },
            {
               "key": "description",
               "regex": r"Description: ?([^\n]*)\n"
            },
            {
                "key": "repository",
                "regex": r"Repository:(</b>)? ?([^\n]*)$",
                "discard": True,
                "keep_in_description": True
            }
        ]

    def map_description(self, split_description):
        description = split_description.get("description")

        # Get rid of multiple " -" which serve as separators
        description = re.sub(r"( +-){2,}", " -", description)

        # Get rid of an initial " -" if one exists
        description = re.sub(r"\A -", "", description, re.MULTILINE)

        # Extracting the title -- from "Title:" to " - ", is possible -- but
        # would require some close analysis to review the results.
        return description


class SdasmValidator(FlickrValidator):
    def __init__(self, **options):
        super().__init__(**options)
        self.add_validatable_field(
            field="identifier",
            validations=[
                SdasmValidator.content_match,
                Validator.verify_type(Validator.list_of(str))
            ]
        )
        self.add_validatable_field(
            field="type",
            validations=[
                Validator.required_field,
                SdasmValidator.type_exception,
                Validator.verify_type(Validator.list_of(str))
            ]

        )
    
    @staticmethod
    def type_exception(validation_def: dict, rikolti_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is equal.

        If content_match validation fails, checks if expected value is
        'Image' and rikolti value is 'image'. 
        """
        content_match_validation = Validator.content_match(
            validation_def, rikolti_value, comparison_value)

        if content_match_validation == "Content mismatch":
            if (comparison_value == ['Image'] and rikolti_value == ['image']):
                return

        return content_match_validation

    @staticmethod
    def content_match(validation_def: dict, rikolti_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is equal.

        If content_match validation fails, strip out identifier labels:
        "Catalog: ", "PictionID: ", "Filename: ", and "SDASM Catalog #: "
        then remove duplicate values, and compare again, disregarding order. 
        """
        content_match_validation = Validator.content_match(
            validation_def, rikolti_value, comparison_value)

        if (content_match_validation == "Content mismatch" 
            and rikolti_value and comparison_value):
            no_rikolti_label = [
                re.sub(
                    r"^(Catalog|PictionID|Filename|SDASM Catalog #): ", "",
                    value
                )
                for value in rikolti_value
            ]
            no_label_no_dupes = list(dict.fromkeys(no_rikolti_label))
            if sorted(no_label_no_dupes) == sorted(comparison_value):
                return

        return content_match_validation


class SdasmVernacular(FlickrVernacular):
    record_cls = SdasmRecord
    validator = SdasmValidator
