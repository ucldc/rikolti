from typing import Any

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator
from ...validator import ValidationLogLevel

class YosemiteRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            'temporal': None,
            'date': self.collate_fields(["available", "created", "dateAccepted",
                                         "dateCopyrighted", "dateSubmitted", "issued",
                                         "modified", "valid", "temporal"])
        }

    def map_is_shown_at(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'nps.gov/npgallery' in i
                  and 'AssetDetail' in i]

        if values:
            return values[-1]

    def map_is_shown_by(self):
        if 'identifier' not in self.source_metadata:
            return

        values = [i for i in filter(None, self.source_metadata.get('identifier'))
                  if 'nps.gov/npgallery' in i
                  and i.endswith('/')]

        if values:
            return values[-1]


class YosemiteValidator(Validator):

    def setup(self):
        self.add_validatable_fields([{
            "field": "is_shown_at",
            "validations": [
                Validator.required_field,
                YosemiteValidator.is_shown_at_improvements,
                Validator.verify_type(str)
            ]
        },
        {
            "field": "is_shown_by",
            "validations": [
                YosemiteValidator.str_match_ignore_url_location,
                Validator.verify_type(str)
            ]
        },
        {
            "field": "identifier",
            "validations": [
                YosemiteValidator.list_match_ignore_url_location,
                Validator.verify_type(Validator.list_of(str))
            ]
        },
        {
            "field": "coverage",
            "validations": [YosemiteValidator.additive_content_match],
            "level": ValidationLogLevel.WARNING
        },
        {
            "field": "spatial",
            "validations": [YosemiteValidator.additive_content_match],
            "level": ValidationLogLevel.WARNING
        }
        ])

    def after_validation(self):
        calisphere_id = self.rikolti_data.get('calisphere-id')
        is_shown_at = self.rikolti_data.get('is_shown_at')
        if calisphere_id and is_shown_at:
            object_id = calisphere_id.rstrip('/').split('/')[-1]
            constructed = (
                f"https://www.nps.gov/npgallery/AssetDetail/{object_id}")
            if is_shown_at.rstrip('/') != constructed:
                self.log.add(
                    key=self.key,
                    field="is_shown_at",
                    description="Doesn't match new URL format",
                    expected=constructed,
                    actual=is_shown_at,
                )

    @staticmethod
    def additive_content_match(validation_def: dict,
                               rikolti_value: Any,
                               comparison_value: Any) -> None:
        """
        This validation checks that the comparison value is a subset of the
        rikolti value. This allows for the rikolti value to have more content
        than the comparison value, but not less.
        """
        if rikolti_value == comparison_value:
            return

        if not set(comparison_value).issubset(set(rikolti_value)):
            return "Content mismatch"

    @staticmethod
    def str_match_ignore_url_location(validation_def: dict,
                                      rikolti_value: Any,
                                      comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return

        if comparison_value and comparison_value.startswith('https://npgallery.nps.gov/'):
            comparison_value = comparison_value.replace(
                'https://npgallery.nps.gov/', 'https://www.nps.gov/npgallery/')

        if not sorted(rikolti_value) == sorted(comparison_value):
            return "Content mismatch"

    @staticmethod
    def list_match_ignore_url_location(validation_def: dict,
                                       rikolti_value: Any,
                                       comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return
        comparison_value = [
            item.replace(
                'https://npgallery.nps.gov/', 'https://www.nps.gov/npgallery/'
            ) if item.startswith('https://npgallery.nps.gov/') else item
            for item in comparison_value
        ]
        if not rikolti_value == comparison_value:
            return "Content mismatch"

    @staticmethod
    def is_shown_at_improvements(validation_def: dict,
                             rikolti_value: Any,
                             comparison_value: Any) -> None:
        """
        is_shown_at used to go to one page for all items in collection 26989.
        Now it goes to the item page.

        This validation confirms that the comparison value is one the known
        collection page url, and if so, checks the rikolti value has a
        proper item page base url. Further checking of the object ID is done in
        after_validation().
        """
        # Old format:
        # 26989: "https://npgallery.nps.gov/AssetDetail/3331008b-6cb0-4583-85f7-b3a93f7e074d",
        # New format:
        # "https://www.nps.gov/npgallery/AssetDetail/040f164d786142e1b0c86893e9e8ee92",

        if rikolti_value == comparison_value:
            return

        url_mismatch = YosemiteValidator.str_match_ignore_url_location(
            validation_def, rikolti_value, comparison_value)
        if not url_mismatch:
            return

        yosemite_collection_urls = [
            "https://npgallery.nps.gov/AssetDetail/3331008b-6cb0-4583-85f7-b3a93f7e074d"
        ]

        if comparison_value in yosemite_collection_urls:
            if not rikolti_value.startswith(
                'https://www.nps.gov/npgallery/AssetDetail/'
            ):
                return "Unrecognized base url"
        else:
            return "Unrecognized solr value"


class YosemiteVernacular(OaiVernacular):
    record_cls = YosemiteRecord
    validator = YosemiteValidator