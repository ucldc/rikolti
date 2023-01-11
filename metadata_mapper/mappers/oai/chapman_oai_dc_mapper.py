from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular

class ChapmanOaiDcRecord(OaiRecord):
    def map_is_shown_at(self) -> Union[str, None]:
        return self.identifier_for_image

    def map_is_shown_by(self) -> Union[str, None]:
        if "type" not in self.source_metadata:
            return

        type: list[str] = self.source_metadata.get("type", [])

        if not type or type[0].lower() != "image":
            return

        url: Union[str, None] = self.identifier_for_image

        return f"{url.replace('items', 'thumbs')}?gallery=preview" if url else None

    def map_description(self) -> Union[str, None]:
        breakpoint()
        return "test"

        if 'description' not in self.source_metadata:
            return

        return [d for d in self.source_metadata.get('description') if 'thumbnail' not in d]

    @property
    def identifier_for_image(self) -> Union[str, None]:
        if "identifier" not in self.source_metadata:
            return

        identifiers = [i for i in self.source_metadata.get('identifier') if "context" not in i]
        return identifiers[0] if identifiers else None

class ChapmanOaiDcVernacular(OaiVernacular):
    record_cls = ChapmanOaiDcRecord