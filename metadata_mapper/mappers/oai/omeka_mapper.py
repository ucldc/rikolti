import requests
from urllib import parse
from typing import Any, Optional

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator
from ...validator import ValidationLogLevel, ValidationMode


class OmekaRecord(OaiRecord):
    """
    TODO: this mapper's `map_is_shown_by` makes a request to generate value in some
    cases
    """
    def UCLDC_map(self):
        return {
            "identifier": self.map_identifier,
            "dateCopyrighted": self.map_rights_date,
        }

    def map_rights_date(self):
        dateCopyrighted = self.source_metadata.get('dateCopyrighted')
        if dateCopyrighted:
            if isinstance(dateCopyrighted, list):
                return dateCopyrighted[0]
            elif isinstance(dateCopyrighted, str):
                return dateCopyrighted

    def map_is_shown_at(self):
        identifiers = [i for i in filter(None, self.source_metadata.get('identifier'))
                       if 'items/show' in i]

        # TODO: This is true to the legacy mapper, but some analysis may be in order
        if identifiers:
            return identifiers[-1]

    def map_is_shown_by(self):
        """
        If this logic changes, those changes may need to be reflected in
        NothumbVernacular.skip()
        """
        identifiers = filter(None, self.source_metadata.get('identifier'))
        for i in identifiers:
            if 's3.amazonaws.com/omeka-net' in i:
                return i
            elif '/files/thumbnails/' in i:
                return i
            elif '/files/original/' in i:
                if i.rsplit('.', 1)[1] == 'jpg':
                    return i
                else:
                    thumb_url = i.replace("/original/", "/thumbnails/")
                    thumb_url = thumb_url.rsplit('.', 1)[0] + '.jpg'
                    request = requests.get(thumb_url)
                    if request.status_code == 200:
                        return thumb_url
                    else:
                        return i

    def map_identifier(self):
        if 'identifier' not in self.source_metadata:
            return

        identifiers = self.source_metadata.get('identifier', [])
        if isinstance(identifiers, str):
            return [identifiers] if 'islandora' not in identifiers else None

        filtered_identifiers = []
        for i in identifiers:
            if "s3.amazonaws.com/omeka-net/" in i:
                continue
            if "files/original" in i:
                continue
            if "items/show" in i:
                continue
            filtered_identifiers.append(i)
        if filtered_identifiers:
            return filtered_identifiers


class OmekaValidator(Validator):

    def setup(self):
        self.add_validatable_fields([
            {
                "field": "is_shown_by",
                "validations": [
                    OmekaValidator.match_signed_s3_url,
                    Validator.verify_type(str)
                ]
            }, {
                "field": "contributor",
                "validations": [OmekaValidator.contributor_match],
                "level": ValidationLogLevel.WARNING
            }, {
                "field": "date",
                "validations": [OmekaValidator.content_match_ignore_n_d],
                "level": ValidationLogLevel.WARNING,
                "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
            }, {
                "field": "description",
                "validations": [OmekaValidator.description_match],
                "level": ValidationLogLevel.WARNING,
            }
        ])

    @staticmethod
    def content_match_ignore_n_d(validation_def: dict,
                                 rikolti_value: Any,
                                 comparison_value: Any) -> Optional[str]:
        """
        in the legacy harvester, we replaced instances of 'N.D.' with None, 
        but in rikolti, Christine's determined that we'd like to keep the 
        source metadata values as-is, though, so a comparison value of None
        and a rikolti value of 'N.D.', 'N.D', or 'N,D' is a match
        """
        if rikolti_value == comparison_value:
            return

        if (
            (not comparison_value) and 
            (len(rikolti_value) == 1) and 
            (rikolti_value[0] in ['N.D.', 'N.D', 'N,D'])
        ):
            return

        return "Content mismatch"

    @staticmethod
    def match_signed_s3_url(validation_def: dict,
                            rikolti_value: Any,
                            comparison_value: Any) -> Optional[str]:
        """
        matches signed s3 urls, despite the fact that their signatures will differ:

        https://s3.amazonaws.com/omeka-net/12811/archive/files/0062fe9e86bd362e6c5cb3ca7949535a.jpg?
            AWSAccessKeyId=AKIASNRMBMX265PGDDPJ&
            Expires=1702512000&
            Signature=kQFWFsxFF1V81OpSEjaLNGJ3Nwo%3D
        https://s3.amazonaws.com/omeka-net/12811/archive/files/0062fe9e86bd362e6c5cb3ca7949535a.jpg?
            AWSAccessKeyId=AKIASNRMBMX265PGDDPJ&
            Expires=1706745600&
            Signature=8DmBsNwc34EiFhDsT%2B%2BHmWiGNm8%3D
        """
        if rikolti_value == comparison_value:
            return

        comparison_url = parse.urlparse(comparison_value)
        rikolti_url = parse.urlparse(rikolti_value)

        if comparison_url.netloc != rikolti_url.netloc:
            return "Content mismatch"
        if comparison_url.path != rikolti_url.path:
            return "Content mismatch"

        comparison_query = parse.parse_qs(comparison_url.query)
        rikolti_query = parse.parse_qs(rikolti_url.query)
        expected_query_keys = ['AWSAccessKeyId', 'Expires', 'Signature']
        if set(comparison_query.keys()) != set(rikolti_query.keys()):
            return "Content mismatch"
        if set(comparison_query.keys()) != set(expected_query_keys):
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

    @staticmethod
    def description_match(validation_def: dict,
                          rikolti_value: Any,
                          comparison_value: Any) -> Optional[str]:
        """ matches values that differ in only trailing space. """
        if rikolti_value == comparison_value:
            return
        
        comparison_value[0] = comparison_value[0].rstrip(' ')
        if rikolti_value == comparison_value:
            return
        
        return "Content mismatch"

class OmekaVernacular(OaiVernacular):
    record_cls = OmekaRecord
    validator = OmekaValidator
