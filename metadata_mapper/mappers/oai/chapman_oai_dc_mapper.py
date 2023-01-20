from typing import Union

from .oai_mapper import OaiRecord, OaiVernacular

class ChapmanOaiDcRecord(OaiRecord):
    """Mapping discrepancies:

        * `type` field for images contains "Image" in Solr, but "text" in mapped data

    """

    def UCLDC_map(self):
        return {
            'description': collate_values([
                self.source_metadata.get('abstract'),
                self.map_description(),
                self.source_metadata.get('tableOfContents')
            ]),
            'identifier': self.map_identifier()
        }

    def map_is_shown_at(self) -> Union[str, None]:
        return self.map_identifier()

    def map_is_shown_by(self) -> Union[str, None]:
        if not self.is_image_type():
            return

        url: Union[str, None] = self.map_identifier()

        return f"{url.replace('items', 'thumbs')}?gallery=preview" if url else None

    def map_description(self) -> Union[str, None]:
        if 'description' not in self.source_metadata:
            return

        return [d for d in self.source_metadata.get('description') if 'thumbnail' not in d]

    def is_image_type(self) -> bool:
        if "type" not in self.source_metadata:
            return False

        type: list[str] = self.source_metadata.get("type", [])

        return type and type[0].lower() == "image"

    def map_identifier(self) -> Union[str, None]:
        if "identifier" not in self.source_metadata:
            return

        identifiers = [i for i in self.source_metadata.get('identifier') if "context" not in i]
        return identifiers[0] if identifiers else None

class ChapmanOaiDcVernacular(OaiVernacular):
    record_cls = ChapmanOaiDcRecord
