from io import StringIO
from typing import Any

from lxml import etree
from pymarc import parse_xml_to_array
from sickle import models

from ..mapper import Vernacular, Validator
from .marc_mapper import MarcRecord

class UcbTindRecord(MarcRecord):

    def UCLDC_map(self):
        self.marc_880_fields = self.get_880_fields()

        return {
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "_id": self.get_marc_data_fields(["901"], ["a"]),
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "alternativeTitle": self.get_marc_data_fields(["246"]),
            "language": self.get_marc_data_fields(["041"], ["a"]),
            "date": self.get_marc_data_fields(["260"], ["c"]),
            "publisher": self.get_marc_data_fields(["260"], ["a", "b"]),
            "format": self.get_marc_data_fields(["655"], ["2"],
                                                exclude_subfields=True),
            "extent": self.map_extent,
            "identifier": self.get_marc_data_fields(["024", "901", "035"],
                                                    ["a"]),
            "contributor": self.get_marc_data_fields(["100", "110", "111"]),
            "creator": self.get_marc_data_fields(["700", "710"], ["a"]),
            "relation": self.map_relation,
            "provenance": self.get_marc_data_fields(["541"], ["a"]),
            "description": self.map_description,
            "rights": self.get_marc_data_fields(["506", "540"]),
            "temporal": self.get_marc_data_fields(["648"]),
            "title": self.map_title,
            "spatial": self.map_spatial,
            "subject": self.map_subject,
            "type": self.get_marc_data_fields(["336"])
        }
            
    def map_is_shown_at(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return "https://digicoll.lib.berkeley.edu/record/" + field_001

    def map_is_shown_by(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return ("https://digicoll.lib.berkeley.edu/nanna/thumbnail/v2/" +
                    field_001 + "?redirect=1")

    def map_spatial(self) -> list:
        f651 = self.get_marc_data_fields(["651"], ["a"])
        additional_fields = [str(i) for i in [600, 630, 650, 651] + list(range(610, 620))
                             + list(range(653, 659)) + list(range(690, 700))]
        values = f651 + self.get_marc_data_fields(additional_fields, ["z"])

        # Stripping off trailing period
        return [value[0:-1] if value[-1] == "." else value for value in values]

    def map_subject(self) -> list:
        fields = [str(i) for i in [600, 630, 650, 651] + list(range(610, 620))
                  + list(range(653, 659)) + list(range(690, 700))]
        return [{"name": s} for s in
                self.get_marc_data_fields(fields, ["2"], exclude_subfields=True)]

    def map_description(self) -> list:
        field_range = [str(i) for i in range(500, 600) if i != 538 and i != 540]

        return self.get_marc_data_fields(field_range, ["a"])

    def map_relation(self) -> list:
        field_range = [str(i) for i in range(760, 788)]  # Up to 787

        self.get_marc_data_fields(field_range)

    def map_extent(self) -> list:
        """
        Retrieves the extent values from MARC field 300 and 340.

        :return: A list of extent values.
        """
        return self.get_marc_data_fields(["300"]) + self.get_marc_data_fields(["340"], ["b"])

    def map_title(self) -> list:
        # 245, all subfields except c
        f245 = self.get_marc_data_fields(["245"], ["c"], exclude_subfields=True)

        # 242, all subfields
        f242 = self.get_marc_data_fields(["242"])

        # 240, all subfields
        f240 = self.get_marc_data_fields(["240"])

        return f245 + f242 + f240


class UcbTindValidator(Validator):

    def setup(self):
        self.add_validatable_fields([
            {
                "field": "is_shown_by",
                "validations": [
                    UcbTindValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            },
            {
                "field": "is_shown_at",
                "validations": [
                    UcbTindValidator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            }
        ])

    @staticmethod
    def str_match_ignore_url_protocol(validation_def: dict,
                                    rikolti_value: Any,
                                    comparison_value: Any) -> None:
        if rikolti_value == comparison_value:
            return

        if comparison_value and comparison_value.startswith('http'):
            comparison_value = comparison_value.replace('http', 'https')

        if not rikolti_value == comparison_value:
            return "Content mismatch"


class UcbTindVernacular(Vernacular):
    record_cls = UcbTindRecord
    validator = UcbTindValidator

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
        for record_element in record_elements:
            sickle_rec = models.Record(record_element)
            sickle_header = sickle_rec.header
            if not sickle_header.deleted:
                marc_record_element = record_element.find(
                    ".//marc:record",
                    namespaces={"marc": "http://www.loc.gov/MARC21/slim"}
                )
                marc_record_string = etree.tostring(
                    marc_record_element,encoding="utf-8").decode("utf-8")

                # Wrap the record in collection so pymarc can read it
                marc_collection_xml_full = \
                    ('<collection xmlns="http://www.loc.gov/MARC21/slim">'
                    f'{marc_record_string}'
                    '</collection>')

                record = {
                    "datestamp": sickle_header.datestamp,
                    "id": sickle_header.identifier,
                    "request_url": request_url,
                    "marc": parse_xml_to_array(StringIO(marc_collection_xml_full))[0]
                }
                records.append(record)

        return self.get_records(records)