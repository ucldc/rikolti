import re
from datetime import datetime
from typing import Any, Optional

from ..mapper import Validator
from ...validator import ValidationLogLevel, ValidationMode
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
            },
            {
                "field": "rights",
                "validations": [
                    SamveraValidator.rights_match,
                    Validator.verify_type(Validator.list_of(str))
                ]
            },
            {
                "field": "date",
                "validations": [SamveraValidator.date_match],
                "level": ValidationLogLevel.WARNING,
            },
            {
                "field": "source",
                "validations": [SamveraValidator.source_match],
                "level": ValidationLogLevel.WARNING
            },
            {
                "field": "description",
                "validations": [Validator.content_match],
                "level": ValidationLogLevel.WARNING,
                "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
            }
        ])

    @staticmethod
    def replace_ursus_with_digital(validation_def: dict,
                                   rikolti_value: Any,
                                   comparison_value: Any) -> Optional[str]:
        if rikolti_value == comparison_value:
            return

        if comparison_value.startswith("https://ursus.library.ucla.edu"):
            comparison_value = comparison_value.replace(
                "https://ursus.library.ucla.edu", 
                "https://digital.library.ucla.edu"
            )

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

        if comparison_value:
            comparison_value = [c + '.' for c in comparison_value]
            if rikolti_value == comparison_value:
                return

        return "Content mismatch"

    @staticmethod
    def rights_match(validation_def: dict,
                     rikolti_value: Any,
                     comparison_value: Any) -> Optional[str]:
        """ 
        matches values that differ only in phone number when new phone number
        is '(310) 825-4988' - in legacy collection 153, this number seemed to
        be auto-incrementing with each record (whoops). example:

        legacy: [
            'US', 
            (
                'UCLA Library Special Collections, A1713 Charles E. Young '
                'Research Library, Box 951575, Los Angeles, CA 90095-1575. '
                'Email: spec-coll@library.ucla.edu. Phone: (310) 825-4987'
                (310) 825-4986
            )
        ]
        rikolti: [
            'US', 
            (
                'UCLA Library Special Collections, A1713 Charles E. Young '
                'Research Library, Box 951575, Los Angeles, CA 90095-1575. '
                'Email: spec-coll@library.ucla.edu. Phone: (310) 825-4988'
            )
        ]
        """
        if rikolti_value == comparison_value:
            return
        new_phone_number = '(310) 825-4988'
        if comparison_value and len(comparison_value) == 2:
            new_comparison_value = re.sub(
                r'\(310\) 825-\d{4}',   # old phone number regex
                new_phone_number,
                comparison_value[1]
            )
            comparison_value[1] = new_comparison_value

            if rikolti_value == comparison_value:
                return

        return "Content mismatch"

    @staticmethod
    def date_match(validation_def: dict,
                   rikolti_value: Any,
                   comparison_value: Any) -> Optional[str]:
        """
        if comparison value is a list of one string date and rikolti
        value is a list of two string dates, one in Month, DD, YYYY
        that matches the comparison value, and the other in YYYY-MM-DD
        that is the same logical date at the comparison value, then
        return None.
        comparison value example: ['August 20, 1951']
        rikolti value example: ['August 20, 1951', '1951-08-20']
        """
        if comparison_value == rikolti_value:
            return

        if not comparison_value or not rikolti_value:
            return "Content mismatch"

        if sorted(rikolti_value) == sorted(comparison_value):
            return

        if len(comparison_value) == 1 and len(rikolti_value) == 2:
            if comparison_value[0] == rikolti_value[0]:
                try:
                    comparison_datetime = datetime.strptime(
                        comparison_value[0], '%B %d, %Y')
                    rikolti_datetime = datetime.strptime(
                        rikolti_value[1], '%Y-%m-%d')
                except ValueError:
                    return "Content mismatch"
                if comparison_datetime == rikolti_datetime:
                    return

        return "Content mismatch"

    @staticmethod
    def source_match(validation_def: dict,
                     rikolti_value: Any,
                     comparison_value: Any) -> Optional[str]:
        """
        matches 
        "['Los Angeles Times Photographic Collection']"
        "['OpenUCLA Collections', 'Los Angeles Times Photographic Collection']"
        """
        if rikolti_value == comparison_value:
            return
        if (
            comparison_value and rikolti_value and 
            len(comparison_value) == 1 and len(rikolti_value) == 2 and
            comparison_value[0] == "Los Angeles Times Photographic Collection"
            and 'OpenUCLA Collections' in rikolti_value and
            'Los Angeles Times Photographic Collection' in rikolti_value
        ):
            return

class SamveraVernacular(OaiVernacular):
    record_cls = SamveraRecord
    validator = SamveraValidator
