from .marc_mapper import MarcRecord, MarcVernacular

from sickle import models
from pymarc import parse_xml_to_array
from lxml import etree
from io import StringIO


class UcbTindRecord(MarcRecord):
    def UCLDC_map(self):
        print({
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "title": self.get_marc_field("245", subfield_key="a"),

            # lambda_shepherd misreports that `marc.ucb_tind not yet implemented` if
            # this isn't here
            "collection": ["collections1", "collections2"],
            "identifier": ["identifier1", "identifier2"]
        })

    def map_is_shown_at(self):
        """
        Can we identify is_shown_at by something about the URL format?
        :return:
        """
        return self.get_marc_field("856", subfield_key="u")

    def map_is_shown_by(self):
        """
        Can we identify is_shown_by by something about the URL format?
        :return:
        """
        return self.get_marc_field("856", subfield_key="u")

    def get_marc_field(self, field_key: str, subfield_key: str = None):
        fields = self.source_metadata.get("fields")
        if not fields:
            return
        matching_fields = []
        for field in fields:
            fk, fv = list(field.items())[0]
            if field_key == fk:
                matching_fields.append(fv)

        if not matching_fields:
            return []

        subfield_values = []
        for field in matching_fields:
            subfields = field.get("subfields")
            for subfield in subfields:
                sk, sv = list(subfield.items())[0]
                if subfield_key == sk or subfield_key is None:
                    subfield_values.append(sv)
        return subfield_values


class UcbTindVernacular(MarcVernacular):
    record_cls = UcbTindRecord

    def _process_record(self, record_element: list, request_url: str) -> UcbTindRecord:
        """
        Process a record element and extract relevant information.

        :param record_element: Element representing a single record.
        :param request_url: The URL of the request.
        :return: A dictionary containing the extracted information from the record.
        """
        marc_record_element = record_element.find(".//marc:record", namespaces={
            "marc": "http://www.loc.gov/MARC21/slim"})
        marc_record_string = etree.tostring(marc_record_element,
                                            encoding="utf-8").decode("utf-8")

        # Wrap the record in collection so pymarc can read it
        marc_collection_xml_full = \
            ('<collection xmlns="http://www.loc.gov/MARC21/slim">'
             f'{marc_record_string}'
             '</collection>')

        record = parse_xml_to_array(StringIO(marc_collection_xml_full))[0].as_dict()

        sickle_rec = models.Record(record_element)
        sickle_header = sickle_rec.header

        if sickle_header.deleted:
            return None

        record["datestamp"] = sickle_header.datestamp
        record["id"] = sickle_header.identifier
        record["request_url"] = request_url

        return record
