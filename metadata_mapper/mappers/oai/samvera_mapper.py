from .oai_mapper import OaiRecord, OaiVernacular


class SamveraRecord(OaiRecord):

    TYPES = {
        "img": "image",
        "txt": "text"
    }

    TYPES_BASEURL = "http://id.loc.gov/vocabulary/resourceTypes/"

    def UCLDC_map(self):
        return {
            "alternativeTitle": self.map_alternative_title,
            "type": self.map_type,
        }

    def map_alternative_title(self) -> list:
        value = self.source_metadata.get("alternative")

        if isinstance(value, list):
            return value

        return [value]

    def map_is_shown_at(self) -> str:
        value = self.source_metadata.get("isShownAt")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None

    def map_is_shown_by(self) -> str:
        value = self.source_metadata.get("object")

        if isinstance(value, list):
            return value[0]
        elif isinstance(value, str):
            return value
        return None

    def map_type(self) -> list:
        """
        Iterates the `type` values. If they are part of the controlled vocabulary
        defined by `TYPES_BASEURL`, then they are mapped to the HR value.

        Returns:
            list
        """
        types = self.source_metadata.get("type", [])
        if isinstance(types, str):
            types = [types]

        type_list = []
        for t in types:
            mapped_type = None
            for (type_k, type_v) in self.TYPES.items():
                if mapped_type:
                    break
                if t == f"{self.TYPES_BASEURL}{type_k}":
                    mapped_type = type_v
            if not mapped_type:
                mapped_type = t.replace(self.TYPES_BASEURL, "")
            type_list.append(mapped_type.lower())
        return type_list


class SamveraVernacular(OaiVernacular):
    record_cls = SamveraRecord
