from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular
from ..mapper import Validator


class CcaVaultRecord(OaiRecord):

    def map_is_shown_at(self) -> Union[str, None]:
        return self.identifier_for_image()

    def map_is_shown_by(self) -> Union[str, None]:
        if not self.is_image_type():
            return

        if not self.source_metadata.get("type", [])[0].lower() != "image":
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

    def generate_keys(self, collection: list[dict], type: str = None,
                      context: dict = {}) -> dict[str, dict]:
        """
        Given a list of records, generates keys and returns a dict with the
        original list contents as values.

        This can be used to override the creation of keys for a given
        mapper to ensure that key intersection works in the
        validate_mapping module.

        Parameters:
            collection: list[dict]
                Data to be added to resulting dict
            type: str (default: None)
                Optional context. Usually this will be used to tell
                this method if it's dealing with Rikolti or Solr data.
            context: dict (default: {})
                Any information needed to perform these calculations.

        Returns: dict[str, dict]
            dict of dicts, keyed to ensure successful intersection.
        """
        if type == "Rikolti":
            # "oai:vault.cca.edu:" is new style, 3433 uses it
            # "oai:cca:" is old style, 26470, 3767, 26391 use it
            # example: 3433--oai:vault.cca.edu:006f6694-a826-6c83-b455-5b62a1ef9d69/1
            # vs: 26470--oai:cca:0029e39d-c3f3-9346-a7d8-6565486a75a8/1
            if context.get('collection_id') == '3433':
                # do the usual thing
                return {
                    f"{context.get('collection_id')}--{r['calisphere-id']}": r
                    for r in collection
                }
            else:
                shimmed_ids = {}
                for r in collection:
                    item_id = r['calisphere-id'].split(':')[-1]
                    rikolti_id = (
                        f"{context.get('collection_id')}--oai:cca:{item_id}"
                    )
                    shimmed_ids[rikolti_id] = r
                return shimmed_ids
        elif type == "Solr":
            return {r['harvest_id_s']: r for r in collection}

class CcaVaultVernacular(OaiVernacular):
    record_cls = CcaVaultRecord
    validator = CcaVaultValidator
