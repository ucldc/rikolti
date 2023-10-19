from typing import Union, Any

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator
from ...validator import ValidationLogLevel, ValidationMode


class CcaVaultRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            "language": self.source_metadata.get("language"),
            "source": self.source_metadata.get("source")
        }

    def map_is_shown_at(self) -> Union[str, None]:
        return self.identifier_for_image()

    def map_is_shown_by(self) -> Union[str, None]:
        if not self.is_image_type():
            return

        base_url: str = self.identifier_for_image()

        return f"{base_url.replace('items', 'thumbs')}?gallery=preview"

    def is_image_type(self) -> bool:
        if "type" not in self.source_metadata:
            return False

        type: list[str] = self.source_metadata.get("type", [])

        return type and type[0].lower() == "image"

    def identifier_for_image(self) -> Union[str, None]:
        identifier: list[str] = self.source_metadata.get("identifier")
        return identifier[0] if identifier else None


class CcaVaultValidator(Validator):

    def __init__(self, **options):
        super().__init__(**options)
        self.add_validatable_field(
            field="is_shown_by", type=str, 
            validations=[
                Validator.required_field,
                CcaVaultValidator.str_match_ignore_url_protocol,
                Validator.type_match
            ])
        self.add_validatable_field(
            field="source", type=str,
            validations=[
                CcaVaultValidator.source_content_match,
            ],
            level=ValidationLogLevel.WARNING
        )
        self.add_validatable_field(
            field="description", type=str,
            validations=[CcaVaultValidator.description_match],
            level=ValidationLogLevel.WARNING,
        )
        # these are all modified to indicate order doesn't matter with
        # otherwise they're the same as the default validator
        # validation_mode=ValidationMode.LAX
        self.add_validatable_field(
            field="temporal", type=str,
            validations=[Validator.content_match],
            level=ValidationLogLevel.WARNING,
            validation_mode=ValidationMode.LAX
        )
        self.add_validatable_field(
            field="date", type=str,
            validations=[Validator.content_match],
            level=ValidationLogLevel.WARNING,
            validation_mode=ValidationMode.LAX
        )
        self.add_validatable_field(
            field="creator", type=str,
            validations=[Validator.content_match],
            level=ValidationLogLevel.WARNING,
            validation_mode=ValidationMode.LAX
        )
        self.add_validatable_field(
            field="format", type=str,
            validations=[Validator.content_match],
            level=ValidationLogLevel.WARNING,
            validation_mode=ValidationMode.LAX
        )

    @staticmethod
    def str_match_ignore_url_protocol(validation_def: dict,
                                    rikolti_value: Any,
                                    comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return

        if comparison_value and comparison_value.startswith('http'):
            comparison_value = comparison_value.replace('http', 'https')

        if not rikolti_value == comparison_value:
            return "Content mismatch"

    # this represents a known improvement in rikolti's mapping logic
    @staticmethod
    def source_content_match(validation_def: dict, rikolti_value: Any,
                             comparison_value: Any) -> None:
        accepted_values = [
            ['Hamaguchi Study Print Collection'],
            ['Capp Street Project Archive'],
            ['CCA/C Archives']
        ]
        if comparison_value is None and rikolti_value in accepted_values:
            return
        else:
            return Validator.content_match(
                validation_def, rikolti_value, comparison_value)

    @staticmethod
    def description_match(validation_def: dict, rikolti_value: Any,
                          comparison_value: Any) -> None:
        if not validation_def["validation_mode"].value.compare(
            rikolti_value, comparison_value):
            new_comparison_value = [v.rstrip("\n ") for v in comparison_value]
            return Validator.content_match(
                validation_def, rikolti_value, new_comparison_value)


class CcaVaultVernacular(OaiVernacular):
    record_cls = CcaVaultRecord
    validator = CcaVaultValidator
