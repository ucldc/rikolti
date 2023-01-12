from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular


class CcaVaultRecord(OaiRecord):

    def UCLDC_map(self) -> dict:
        return {
            "subject": self.map_subject()
        }

    def map_subject(self) -> Union[list[dict[str, str]], None]:
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127
        value = self.source_metadata.get('subject')
        if not value:
            return None

        if isinstance(value, str):
            value = [value]
        return [{'name': v} for v in value if v]

    def map_is_shown_at(self) -> Union[str, None]:
        return self.transform_identifier()

    def map_is_shown_by(self) -> Union[str, None]:
        if "type" in self.source_metadata:
            if self.source_metadata.get("type", [])[0].lower() == "image":
                base_url: str = self.transform_identifier()
                return f"{base_url.replace('items', 'thumbs')}?gallery=preview"

    def transform_identifier(self) -> Union[str, None]:
        identifier: list[str] = self.source_metadata.get("identifier")
        return identifier[0] if identifier else None


class CcaVaultVernacular(OaiVernacular):
    record_cls = CcaVaultRecord
