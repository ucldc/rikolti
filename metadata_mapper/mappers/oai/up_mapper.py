from typing import Any


from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator


class UpRecord(OaiRecord):
    def map_is_shown_by(self):
        if 'description' not in self.source_metadata:
            return

        values = [u.replace('thumbnail', 'preview')
                  for u in filter(None, self.source_metadata.get('description'))]

        if values:
            return values[0]

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'scholarlycommons.pacific.edu' in i and 'viewcontent' not in i]

        if values:
            return values[-1]

    def map_description(self):
        if 'description' not in self.source_metadata:
            return

        return [d for d in filter(None, self.source_metadata.get('description'))
                if 'thumbnail.jpg' not in d]


class UpValidator(Validator):
    def setup(self):
        self.add_validatable_fields([
            {
                "field": "is_shown_at",
                "validations": [
                    Validator.required_field,
                    UpValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ],
            },
            {
                "field": "is_shown_by",
                "validations": [
                    UpValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            },
        ])

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


class UpVernacular(OaiVernacular):
    record_cls = UpRecord
    validator = UpValidator
