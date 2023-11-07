from typing import Union

from lxml import etree
from sickle import models

from ..mapper import Record, Vernacular


class OaiRecord(Record):
    """Superclass for OAI metadata."""

    def UCLDC_map(self):
        return {
            # `legacy_couch_db_id` is set by a premapping function
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
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
            
            "spatial": self.collate_fields(["coverage", "spatial"]),
            "subject": self.map_subject,
            "temporal": self.source_metadata.get("temporal"),
            "title": self.source_metadata.get("title"),
            "type": self.source_metadata.get("type")
        }

    def map_subject(self) -> Union[list[dict[str, str]], None]:
        # https://github.com/calisphere-legacy-harvester/dpla-ingestion/blob/ucldc/lib/mappers/dublin_core_mapper.py#L117-L127 # noqa: E501
        value = self.source_metadata.get("subject")
        if not value:
            return None

        if isinstance(value, str):
            value = [value]
        return [{"name": v} for v in value if v]

    def map_is_shown_at(self):
        """Will be implemented by child mapper classes"""
        return

    def map_is_shown_by(self):
        """Will be implemented by child mapper classes"""
        return


class OaiVernacular(Vernacular):

    def parse(self, api_response):
        api_response = bytes(api_response, "utf-8")
        namespace = {"oai2": "http://www.openarchives.org/OAI/2.0/"}
        page = etree.XML(api_response)

        request_elem = page.find("oai2:request", namespace)
        if request_elem is not None:
            request_url = request_elem.text
        else:
            request_url = None

        record_elements = (
            page
            .find("oai2:ListRecords", namespace)
            .findall("oai2:record", namespace)
        )

        records = []
        for re in record_elements:
            sickle_rec = models.Record(re)
            sickle_header = sickle_rec.header
            if sickle_header.deleted:
                continue

            record = self.strip_metadata(sickle_rec.metadata)
            record["datestamp"] = sickle_header.datestamp
            record["id"] = sickle_header.identifier
            record["request_url"] = request_url
            records.append(record)

        return self.get_records(records)

    def strip_metadata(self, record_metadata):
        stripped = {}
        for key, value in record_metadata.items():
            if isinstance(value, str):
                value = value.strip()
            elif isinstance(value, list):
                value = [v.strip() if isinstance(v, str) else v for v in value]
            stripped[key] = value

        return stripped
