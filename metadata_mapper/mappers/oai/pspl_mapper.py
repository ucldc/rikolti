from typing import Any


from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator


class PsplRecord(OaiRecord):
    def map_is_shown_by(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        _, record_id = identifier.rsplit(':', 1)
        return f"https://collections.accessingthepast.org/?a=is&oid={record_id}.1.1"\
               "&type=pagethumbnailimage&width=200"

    def map_is_shown_at(self):
        identifier = self.source_metadata.get('id')
        if ':' not in identifier:
            return

        collection_id, record_id = identifier.rsplit(':', 1)
        return f"https://collections.accessingthepast.org/cgi-bin/{collection_id}?a=d&d={record_id}"


class PsplValidator(Validator):
    def setup(self):
        self.add_validatable_fields(
            {
                "field": "is_shown_at",
                "validations": [
                    Validator.required_field,
                    PsplValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ],
            },
            {
                "field": "is_shown_by",
                "validations": [
                    PsplValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            },
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


class PsplVernacular(OaiVernacular):
    record_cls = PsplRecord
    validator = PsplValidator