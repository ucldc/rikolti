from typing import Union, Optional, Any

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator

from rikolti.utils.request_retry import configure_http_session

class IslandoraRecord(OaiRecord):
    # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/islandora_oai_dc_mapper.py
    # https://github.com/ucldc/harvester/blob/master/harvester/fetcher/oai_fetcher.py
    def UCLDC_map(self):
        return {
            'spatial': self.map_spatial
        }

    def map_spatial(self) -> Union[list[str], None]:
        spatial = self.collate_fields(["coverage", "spatial"])()
        spatial = [s for s in spatial if s]
        split_spatial = []
        for value in spatial:
            split_spatial.extend(value.split(';'))

        return [val.strip() for val in split_spatial if val]

    def map_subject(self):
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127
        value = self.source_metadata.get('subject')
        if value:
            if isinstance(value, str):
                return [{'name': value}]
            else:
                return [{'name': v} for v in value if v]
        else:
            return None

    def map_is_shown_at(self):
        oai_url = self.source_metadata.get('request_url', '')
        request_url = oai_url.replace('/oai2', '')

        identifier = self.source_metadata.get('id', '')
        _, record_id = identifier.rsplit(':', 1)
        new_id = record_id.replace('_', '%3A', 1)

        if request_url and new_id:
            return f"{request_url}/islandora/object/{new_id}"
        else:
            return None

    def map_is_shown_by(self):
        oai_url = self.source_metadata.get('request_url', '')
        request_url = oai_url.replace('/oai2', '')

        identifier = self.source_metadata.get('id', '')
        _, record_id = identifier.rsplit(':', 1)
        new_id = record_id.replace('_', '%3A', 1)

        if not (request_url and new_id):
            return None

        thumb_url = (
            f"{request_url}/islandora/object/{new_id}/datastream/TN/view")

        # Change URL from 'TN' to 'JPG' for larger versions of image
        # objects & test to make sure the link resolves
        if 'image' or 'StillImage' in self.source_metadata.get('type', ''):
            jpg_url = thumb_url.replace("/TN/", "/JPG/")
            # TODO: should figure out a way to punt a request
            # to minimize the mapper's reliance on external systems
            http = configure_http_session()
            request = http.get(jpg_url)
            if request.status_code == 200:
                thumb_url = jpg_url

        return thumb_url


class IslandoraValidator(Validator):
    def setup(self):
        self.add_validatable_fields([{
            "field": "identifier",
            "validations": [
                            IslandoraValidator.order_and_space_insensitive_dedupe_match,
                            Validator.verify_type(Validator.list_of(str))
                            ],
        },{
        "field": "rights",
        "validations": [
                        IslandoraValidator.rights_match_ignore_suffix,
                        Validator.verify_type(Validator.list_of(str))
                        ]
        }])

    @staticmethod
    def order_and_space_insensitive_dedupe_match(
            validation_def: dict, rikolti_value: Any,
            comparison_value: Any
        ) -> Optional[str]:
        """
        matches ['islandora:52_0', 'filename: cartaz_030', 'islandora: 52_0']
        with    ['filename: cartaz_030', 'islandora: 52_0']"""
        if sorted(rikolti_value) == sorted(comparison_value):
            return None

        rikolti_value = [v.replace(" ", "") for v in rikolti_value]
        comparison_value = [v.replace(" ", "") for v in comparison_value]
        if (
            sorted(list(set(rikolti_value))) == 
            sorted(list(set(comparison_value)))
        ):
            return None

        return "Content mismatch"

    @staticmethod
    def rights_match_ignore_suffix(validation_def: dict, rikolti_value: Any,
                                   comparison_value: Any) -> Optional[str]:
        suffix = (
            " For more information on copyright or permissions for this "
            "image, please contact San Jose State University Special "
            "Collections & Archives department. "
            "http://www.sjlibrary.org/research/special/special_coll/index.htm"
        )
        if rikolti_value == comparison_value:
            return None
        if (
            rikolti_value and len(rikolti_value) == 1 and 
            comparison_value and len(comparison_value) == 1 and
            rikolti_value[0] + suffix == comparison_value[0]
        ):
            return None
        return "Content mismatch"

class IslandoraVernacular(OaiVernacular):
    record_cls = IslandoraRecord
    validator = IslandoraValidator
