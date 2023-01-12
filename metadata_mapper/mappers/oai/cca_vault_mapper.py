from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular


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


class CcaVaultVernacular(OaiVernacular):
    record_cls = CcaVaultRecord
