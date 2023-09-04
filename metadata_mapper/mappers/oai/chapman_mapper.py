from typing import Union, Any

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator


class ChapmanRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            "description": self.map_description,
            "identifier": self.map_identifier,
            "spatial": self.map_spatial
        }

    def map_is_shown_at(self) -> Union[str, None]:
        identifiers = self.map_identifier()
        return identifiers[0] if identifiers else None

    def map_is_shown_by(self) -> Union[str, None]:
        if not self.is_image_type():
            return

        description = [d for d in self.source_metadata.get("description")
                       if "thumbnail" in d]
        if not description:
            return

        return description[0].replace("thumbnail", "preview")

    def map_description(self) -> Union[str, None]:
        description = [d for d in self.source_metadata.get("description")
                       if "thumbnail" not in d]
        aggregate = [
            self.source_metadata.get("abstract"),
            description[0] if description else None,
            self.source_metadata.get("tableOfContents")
        ]

        return [v for v in filter(bool, aggregate)]

    def map_spatial(self):
        spatial = []
        for field in ["coverage", "spatial"]:
            value = self.split_and_flatten(field)()
            if value:
                spatial.extend(value)

        return spatial

    def is_image_type(self) -> bool:
        """
        The `type` field has the value `text` for many images, so it's a useless
        indicator of whether its an image or not
        """
        if "format" not in self.source_metadata:
            return False

        type: list[str] = self.source_metadata.get("format", [])

        return type and type[0].lower().startswith("image")

    def map_identifier(self) -> Union[str, None]:
        if "identifier" not in self.source_metadata:
            return

        identifiers = [i for i in self.source_metadata.get("identifier")
                       if "context" not in i]

        return identifiers


class ChapmanValidator(Validator):
    def setup(self, **options):
        self.add_validatable_field(
            field="identifier", type=Validator.list_of(str),
            validations=[
                ChapmanValidator.list_match_ignore_url_protocol,
                Validator.type_match,
            ]
        )
        self.add_validatable_field(
            field="is_shown_at", type=str,
            validations=[
                Validator.required_field,
                ChapmanValidator.str_match_ignore_url_protocol,
                Validator.type_match,
            ]
        )
        self.add_validatable_field(
            field="is_shown_by", type=str,
            validations=[
                Validator.required_field,
                ChapmanValidator.str_match_ignore_url_protocol,
                Validator.type_match,
            ]
        )

    @staticmethod
    def list_match_ignore_url_protocol(validation_def: dict,
                                       rikolti_value: Any,
                                       comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return

        comparison_value = [comparison_item.replace("http", "https") if
                            comparison_item.startswith("http") else comparison_item for
                            comparison_item in comparison_value]

        if not rikolti_value == comparison_value:
            return "Content mismatch"


    @staticmethod
    def str_match_ignore_url_protocol(validation_def: dict,
                                      rikolti_value: Any,
                                      comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return

        if comparison_value.startswith("http"):
            comparison_value = comparison_value.replace("http", "https")
        if not rikolti_value == comparison_value:
            return "Content mismatch"


class ChapmanVernacular(OaiVernacular):
    record_cls = ChapmanRecord
    validator = ChapmanValidator
