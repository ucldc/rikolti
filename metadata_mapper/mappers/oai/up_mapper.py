import re
from typing import Any, Optional


from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator


class UpRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            "description": self.map_description
        }

    def map_is_shown_by(self):
        if 'description' not in self.source_metadata:
            return

        values = [u.replace('thumbnail', 'preview')
                  for u in self.source_metadata.get('description', [])
                  if 'thumbnail.jpg' in u]

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
            {
                "field": "identifier",
                "validations": [
                    UpValidator.identifier_ignore_url_template_match,
                    Validator.verify_type(Validator.list_of(str))
                ]
            }
        ])

    @staticmethod
    def str_match_ignore_url_protocol(validation_def: dict,
                                    rikolti_value: Any,
                                    comparison_value: Any) -> Optional[str]:
        if rikolti_value == comparison_value:
            return

        if comparison_value and comparison_value.startswith('http'):
            comparison_value = comparison_value.replace('http', 'https')

        if not rikolti_value == comparison_value:
            return "Content mismatch"


    @staticmethod
    def identifier_ignore_url_template_match(validation_def: dict,
                                             rikolti_value: Any,
                                             comparison_value: Any) -> Optional[str]:
        if rikolti_value == comparison_value:
            return

        if len(comparison_value) != len(rikolti_value):
            return "Content mismatch"

        comparison_value = [comparison_item.replace("http:", "https:") if
                            comparison_item.startswith("http:") else comparison_item for
                            comparison_item in comparison_value]
        if comparison_value == rikolti_value:
            return

        for i in range(len(comparison_value)):
            if comparison_value[i] == rikolti_value[i]:
                continue

            image_match = re.match(
                comparison_value[i] + r"/(\w+).(?:jpg|png|JPG|tif)",
                rikolti_value[i]
            )
            if image_match:
                continue

            comparison_match = re.match(
                (
                    r"https://scholarlycommons.pacific.edu/cgi/"
                    r"viewcontent.cgi\?article=(?P<article>\d+)(?:&|&amp;)"
                    r"context=(?P<context>\w+)"
                ),
                comparison_value[i]
            )
            rikolti_match = re.match(
                (
                    r"https://scholarlycommons.pacific.edu/"
                    r"context/(?P<context>\w+)/article/(?P<article>\d+)"
                ),
                rikolti_value[i]
            )
            if (comparison_match and rikolti_match and
                comparison_match.groupdict() == rikolti_match.groupdict()):
                continue

            return "Content mismatch"


class UpVernacular(OaiVernacular):
    record_cls = UpRecord
    validator = UpValidator
