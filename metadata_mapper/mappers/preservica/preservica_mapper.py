import re
from xml.etree import ElementTree

from ..mapper import Record, Vernacular


class PreservicaRecord(Record):
    BASE_URL = "https://oakland.access.preservica.com"

    FILE_PREPEND = "sdb:digitalFile%7C"

    def UCLDC_map(self):
        entity_id = self.source_metadata.get("entity_id")

        return {
            "calisphere-id": entity_id,
            "contributor": self.source_metadata.get("contributor"),
            "coverage": self.source_metadata.get("spatial"),
            "creator": self.source_metadata.get("creator"),
            "date": self.source_metadata.get("date"),
            "description": self.source_metadata.get("description"),
            "format": self.source_metadata.get("format"),
            "identifier": self.source_metadata.get("identifier"),
            "isShownAt": (
                f"{self.BASE_URL}/file/{self.FILE_PREPEND}{entity_id}/"
            ),
            "isShownBy": (
                f"{self.BASE_URL}/download/thumbnail/{self.FILE_PREPEND}{entity_id}"
            ),
            "language": self.source_metadata.get("language"),
            "publisher": self.source_metadata.get("publisher"),
            "relation": self.source_metadata.get("relation"),
            "rights": self.source_metadata.get("rights"),
            "source": self.source_metadata.get("source"),
            "state_located_in": {"stateLocatedIn": "California"},
            "subject": self.source_metadata.get("subject"),
            "title": self.source_metadata.get("title"),
            "type": self.source_metadata.get("type"),
        }


class PreservicaVernacular(Vernacular):
    record_cls = PreservicaRecord

    # TODO: consider putting this namespace mapping in a place that can be imported
    # into both the mapper and fetcher
    NAMESPACES: dict = {
        "pra": "http://preservica.com/EntityAPI/v6.0",
        "xip": "http://preservica.com/XIP/v6.0",
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"
    }

    def parse(self, response_body):
        """
        We expect only one record per file for preservica. Minor changes will need to
        be made if we begin importing more per page.
        """
        et = ElementTree.fromstring(response_body)
        container = et.find(".//xip:MetadataContainer", self.NAMESPACES)

        dc_record = container.find("xip:Content", self.NAMESPACES).\
            find("oai_dc:dc", self.NAMESPACES)

        record = {
            "entity_id": container.find("xip:Entity", self.NAMESPACES).text,
        }
        for element in dc_record:
            key = re.sub(r"{\S+}", "", element.tag)  # Strip the namespace off the tag
            value = element.text
            if key not in record:
                record[key] = []
            record[key].append(value)

        return self.get_records([record])
