from collections import defaultdict
import re

from lxml import etree

from ..islandora_mapper import IslandoraRecord, IslandoraVernacular

class ArchipelagoRecord(IslandoraRecord):

    def UCLDC_map(self):
        return {
            'isShownAt': self.source_metadata.get('isShownAt'),
            'isShownBy': self.source_metadata.get('isShownBy')
        }

class ArchipelagoVernacular(IslandoraVernacular):
    record_cls = ArchipelagoRecord

    def parse(self, api_response):
        api_response = bytes(api_response, "utf-8")
        namespace = {
            "oai2": "http://www.openarchives.org/OAI/2.0/",
            "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"
        }
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
        for record_element in record_elements:
            header = record_element.find("oai2:header", namespace)
            if header.attrib.get('status') == 'deleted':
                continue

            metadata_elements = (
                record_element
                .find("oai2:metadata", namespace)
                .find("oai_dc:dc", namespace)
                .getchildren()
            )

            fields = defaultdict(list)
            for element in metadata_elements:
                tag = re.sub(r'\{.*\}', '', element.tag)
                if tag == 'identifier.url':
                    fields['isShownAt'].append(element.text)
                elif tag == 'identifier.thumbnail':
                    fields['isShownBy'].append(element.text)
                fields[tag].append(element.text)

            fields["datestamp"] = header.find("oai2:datestamp", namespace).text
            fields["id"] = header.find("oai2:identifier", namespace).text
            fields["request_url"] = request_url

            record = dict(fields)
            record = self.strip_metadata(record)

            records.append(record)

        return self.get_records(records)
