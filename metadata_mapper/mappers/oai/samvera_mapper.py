from typing import Any, Optional

from ..mapper import Validator
from ...validator import ValidationLogLevel
from .oai_mapper import OaiRecord, OaiVernacular


class SamveraRecord(OaiRecord):

    def map_is_shown_at(self) -> Optional[str]:
        value = self.source_metadata.get("isShownAt")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None

    def map_is_shown_by(self) -> Optional[str]:
        value = self.source_metadata.get("object")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None

class SamveraValidator(Validator):
    def setup(self):
        self.add_validatable_fields([
            {
                "field": "is_shown_at",
                "validations": [
                    SamveraValidator.replace_ursus_with_digital,
                    Validator.verify_type(str)
                ]
            },
            {
                "field": "contributor",
                "validations": [SamveraValidator.contributor_match],
                "level": ValidationLogLevel.WARNING
            }
        ])

    @staticmethod
    def replace_ursus_with_digital(validation_def: dict,
                                   rikolti_value: Any,
                                   comparison_value: Any) -> Optional[str]:
        if rikolti_value == comparison_value:
            return

        if comparison_value.startswith("https://ursus.library.ucla.edu"):
            comparison_value.replace("https://ursus.library.ucla.edu", "https://digital.library.ucla.edu")

        if rikolti_value == comparison_value:
            return

        return "Content mismatch"

    @staticmethod
    def contributor_match(validation_def: dict,
                          rikolti_value: Any,
                          comparison_value: Any) -> Optional[str]:
        """ matches values that differ in only a trailing period. """
        if rikolti_value == comparison_value:
            return

        comparison_value[0] = comparison_value[0] + '.'
        if rikolti_value == comparison_value:
            return

        return "Content mismatch"

class SamveraVernacular(OaiVernacular):
    record_cls = SamveraRecord
    validator = SamveraValidator
