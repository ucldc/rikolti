from typing import Union, Any, Optional

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator
from ...validator import ValidationLogLevel


class QuartexRecord(OaiRecord):
    def UCLDC_map(self):
        return {
            'spatial': self.map_spatial,
            'language': self.map_language,
            'contributor': self.split_and_flatten("contributor"),
        }

    def map_language(self) -> list:
        languages = self.source_metadata.get("language")
        if not isinstance(languages, list):
            languages = [languages]
        languages = [language for language in languages if language]
        split_languages = []
        for value in languages:
            split_languages.extend(value.split(';'))

        return [val.strip() for val in split_languages if val]

    def map_spatial(self) -> Union[list[str], None]:
        spatial = self.collate_fields(["coverage", "spatial"])()
        spatial = [s for s in spatial if s]
        split_spatial = []
        for value in spatial:
            split_spatial.extend(value.split(';'))

        return [val.strip() for val in split_spatial if val]

    def map_subject(self) -> Union[list[dict[str, str]], None]:
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127 # noqa: E501
        value = self.source_metadata.get("subject")
        if not value:
            return None

        if isinstance(value, str):
            value = [value]

        split_subjects = []
        for v in value:
            split_subjects.extend(v.split(';'))

        return [{"name": v.strip()} for v in split_subjects if v]

    def map_is_shown_at(self):
        if "identifier" not in self.source_metadata:
            return

        is_shown_at = [i for i in self.source_metadata.get("identifier")
                       if "documents/detail" in i]

        if is_shown_at:
            return is_shown_at[-1]

    def map_is_shown_by(self):
        """
        TODO: determine if the following comment from the legacy mapper needs to be
              implemented (code both with and without follows):

              # Grab the image URL from identifier values and switch out Size2 for Size4
              # (largest possible)
        """

        values = self.source_metadata.get("identifier")
        if not values:
            return

        candidates = [v for v in values if "thumbnails/preview" in v]
        if not candidates:
            return

        return candidates[-1]
        # OR do the replacement that the legacy mapper describes but doesn't implement
        # return candidates[-1].replace("Size2", "Size4")


class QuartexValidator(Validator):

    def setup(self):
        self.add_validatable_fields([
            {
                "field": "creator",
                "validations": [QuartexValidator.match_trailing_periods],
                "level": ValidationLogLevel.WARNING,
            },
            {
                "field": "contributor",
                "validations": [QuartexValidator.match_trailing_periods],
                "level": ValidationLogLevel.WARNING,
            },
            {
                "field": "description",
                "validations": [QuartexValidator.match_trailing_space],
                "level": ValidationLogLevel.WARNING,
            }
        ])


    @staticmethod
    def match_trailing_periods(validation_def: dict,
                               rikolti_value: Any,
                               comparison_value: Any) -> Optional[str]:
        """
        Matches solr values ["Wieliczka, Amy L"] with rikolti values ["Wieliczka, Amy L."]
        """
        if rikolti_value == comparison_value:
            return

        if (
            rikolti_value and comparison_value and 
            len(rikolti_value) == len(comparison_value)
        ):
            rikolti_value = sorted(rikolti_value)
            comparison_value = sorted(comparison_value)
            for i in range(len(rikolti_value)):
                if (
                    rikolti_value[i] != comparison_value[i] and
                    rikolti_value[i] != comparison_value[i] + '.'
                ):
                    return "Content mismatch"
            return

        return "Content mismatch"

    @staticmethod
    def match_trailing_space(validation_def: dict,
                             rikolti_value: Any,
                             comparison_value: Any) -> Optional[str]:
        """
        Matches solr values ["Wieliczka, Amy L"] with rikolti values ["Wieliczka, Amy L "]
        """
        if rikolti_value == comparison_value:
            return

        if (
            rikolti_value and comparison_value and 
            len(rikolti_value) == len(comparison_value)
        ):
            rikolti_value = sorted(rikolti_value)
            comparison_value = sorted(comparison_value)
            for i in range(len(rikolti_value)):
                if (
                    rikolti_value[i] != comparison_value[i] and
                    rikolti_value[i] + ' ' != comparison_value[i]
                ):
                    return "Content mismatch"
            return

        return "Content mismatch"

class QuartexVernacular(OaiVernacular):
    record_cls = QuartexRecord
    validator = QuartexValidator
