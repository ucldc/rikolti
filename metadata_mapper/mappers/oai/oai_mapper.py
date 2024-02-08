from typing import Union, Optional

import requests
from lxml import etree
from sickle import models

from ..mapper import Record, Vernacular


class OaiRecord(Record):
    """Superclass for OAI metadata."""

    TYPES = {
        "img": "image",
        "txt": "text"
    }

    TYPES_BASEURL = "http://id.loc.gov/vocabulary/resourceTypes/"

    def UCLDC_map(self):
        return {
            # `legacy_couch_db_id` is set by a premapping function
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "alternativeTitle": self.map_alternative_title,
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "contributor": self.source_metadata.get("contributor"),
            "creator": self.source_metadata.get("creator"),
            "date": self.collate_fields([
                "available",
                "created",
                "date",
                "dateAccepted",
                "dateCopyrighted",
                "dateSubmitted",
                "issued",
                "modified",
                "valid"
            ]),
            "description": self.collate_fields(["abstract", "description",
                                                "tableOfContents"]),
            "extent": self.source_metadata.get("extent"),
            "format": self.collate_fields(["format", "medium"]),
            "identifier": self.collate_fields(["bibliographicCitation", "identifier"]),
            "language": self.map_language,
            "provenance": self.source_metadata.get("provenance"),
            "publisher": self.source_metadata.get("publisher"),
            "relation": self.collate_fields([
                "conformsTo",
                "hasFormat",
                "hasPart",
                "hasVersion",
                "isFormatOf",
                "isPartOf",
                "isReferencedBy",
                "isReplacedBy",
                "isRequiredBy",
                "isVersionOf",
                "references",
                "relation",
                "replaces",
                "require"
            ]),
            "rights": self.collate_fields(["accessRights", "rights"]),
            "source": self.source_metadata.get("source"),
            "spatial": self.collate_fields(["coverage", "spatial"]),
            "subject": self.map_subject,
            "temporal": self.source_metadata.get("temporal"),
            "title": self.source_metadata.get("title"),
            "type": self.source_metadata.get("type")
        }

    def map_alternative_title(self) -> list:
        value = self.source_metadata.get("alternative", [])

        if isinstance(value, list):
            return value

        return [value]

    def map_is_shown_at(self):
        """Will be implemented by child mapper classes"""
        return

    def map_is_shown_by(self):
        """Will be implemented by child mapper classes"""
        return

    def map_subject(self) -> Union[list[dict[str, str]], None]:
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127 # noqa: E501
        value = self.source_metadata.get("subject")
        if not value:
            return None

        if isinstance(value, str):
            value = [value]
        return [{"name": v} for v in value if v]

    def map_language(self) -> list:
        value = self.source_metadata.get("language")

        if isinstance(value, list):
            return value

        return [value]

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

        return types
        # type_list = []
        # for t in types:
        #     mapped_type = None
        #     for (type_k, type_v) in self.TYPES.items():
        #         if mapped_type:
        #             break
        #         if t == f"{self.TYPES_BASEURL}{type_k}":
        #             mapped_type = type_v
        #     if not mapped_type:
        #         mapped_type = t.replace(self.TYPES_BASEURL, "")
        #     type_list.append(mapped_type.lower())
        # return type_list


class OaiVernacular(Vernacular):
    namespaces = {"oai2": "http://www.openarchives.org/OAI/2.0/"}

    def parse(self, api_response: str) -> list[Record]:
        api_response_b = bytes(api_response, "utf-8")
        page = etree.XML(api_response_b)

        request_elem = page.find("oai2:request", namespaces=self.namespaces)
        request_url = request_elem.text if request_elem is not None else None

        record_elements = (
            page
            .find("oai2:ListRecords", namespaces=self.namespaces)
            .findall("oai2:record", namespaces=self.namespaces)
        )

        records = [self._process_record(re, request_url)
                   for re in record_elements]
        records = list(filter(None, records))

        return self.get_records(records)


    def _process_record(self,
                        record_element: etree.ElementBase,
                        request_url: Optional[str]) -> Optional[dict]:
        sickle_rec = models.Record(record_element)
        sickle_header = sickle_rec.header

        if sickle_header.deleted:
            return None

        record = self.strip_metadata(sickle_rec.metadata)
        record["datestamp"] = sickle_header.datestamp
        record["id"] = sickle_header.identifier
        record["request_url"] = request_url

        return record

    def strip_metadata(self, record_metadata: dict) -> dict:
        stripped = {}
        for key, value in record_metadata.items():
            if isinstance(value, str):
                value = value.strip()
            elif isinstance(value, list):
                value = [v.strip() if isinstance(v, str) else v for v in value]
            stripped[key] = value

        return stripped
