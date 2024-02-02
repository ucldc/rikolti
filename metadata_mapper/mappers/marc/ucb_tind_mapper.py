from .marc_mapper import MarcRecord, MarcVernacular
from ..oai.oai_mapper import OaiVernacular

from sickle import models
from pymarc import parse_xml_to_array
from lxml import etree
from io import StringIO

from ..mapper import Validator

from collections import OrderedDict

import re


class UcbTindRecord(MarcRecord):
    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "_id": self.get_marc_data_fields(["901"], ["a"]),
            "isShownAt": self.map_is_shown_at,
            "isShownBy": self.map_is_shown_by,
            "alt_title": self.get_marc_data_fields(["246"], ["6"],
                                                   exclude_subfields=True),
            "language": self.get_marc_data_fields(["041"], ["a"]),
            "date": self.get_marc_data_fields(["260"], ["c"]),
            "publisher": self.get_marc_data_fields(["260"], ["a", "b"]),
            "format": self.get_marc_data_fields(["655"], ["2"],
                                                exclude_subfields=True),
            "extent": self.map_extent,
            "identifier": self.get_marc_data_fields(["024", "901", "035"],
                                                    ["a"]),
            "contributor": self.get_marc_data_fields(["100", "110", "111"]),
            "creator": self.get_marc_data_fields(["700"], ["a"]),
            "relation": self.map_relation,
            "provenance": self.get_marc_data_fields(["541"], ["a"]),
            "description": self.map_description,
            "rights": self.get_marc_data_fields(["506", "540"]),
            "temporal": self.get_marc_data_fields(["648"]),
            "title": self.map_title,
            "spatial": self.map_spatial,
            "spec_type": self.map_spec_type,
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

    def map_spec_type(self):
        value = []

        for types in self.get_matching_types():
            value.append(types[0])

        if (self.get_marc_control_field("008", 28)
                in ("a", "c", "f", "i", "l", "m", "o", "s") or
                self.get_marc_data_fields(["086", "087"])):
            value.append("Government Document")

        return value

    def get_matching_types(self):
        type_mapping = []
        compare = (self.get_marc_leader("type_of_control") +
                   self.get_marc_leader("bibliographic_level") +
                   self.get_marc_control_field("007", 1) +
                   self.get_marc_control_field("008", 21))

        for (key, value) in self.get_types()["leader"].items():
            if re.match(f"^{key}", compare):
                type_mapping.append(value)

        return type_mapping

    def get_metadata_fields(self):
        """
        Returns a list of metadata fields used by map_format, map_subject,
        map_temporal, map_format

        :return: A list of fields
        :rtype: list
        """
        return [str(i) for i in [600, 630, 650, 651] + list(range(610, 620)) + list(
            range(653, 659)) + list(range(690, 700))]

    def map_spatial(self) -> list:
        f651 = self.get_marc_data_fields(["651"], ["a"])

        return f651 + self.get_marc_data_fields(self.get_metadata_fields(), ["z"])

    def map_subject(self) -> list:
        fields = self.get_metadata_fields()
        return [{"name": s} for s in self.get_marc_data_fields(fields, ["2"], exclude_subfields=True)]

    def map_temporal(self) -> list:
        f648 = self.get_marc_data_fields(["648"])

        return f648 + self.get_marc_data_fields(self.get_metadata_fields(), ["y"])

    def map_format(self) -> list:
        return self.get_marc_data_fields(self.get_metadata_fields(), ["v"])

    def map_description(self) -> list:
        field_range = [str(i) for i in range(500, 600) if i != 538 and i != 540]

        return self.get_marc_data_fields(field_range, ["a"])

    def map_relation(self) -> list:
        field_range = [str(i) for i in range(760, 788)]  # Up to 787

        self.get_marc_data_fields(field_range)

    def map_identifier(self) -> list:
        f050 = self.get_marc_data_fields(["050"], ["a", "b"])

        return f050 + self.get_marc_data_fields(["020", "022", "035"], ["a"])

    def map_extent(self) -> list:
        """
        Retrieves the extent values from MARC field 300 and 340.

        :return: A list of extent values.
        """
        return [", ".join(self.get_marc_data_fields(["300"]) + self.get_marc_data_fields(["340"],
                                                                              ["b"]))]

    def map_title(self) -> list:
        # 245, all subfields except c
        f245 = self.get_marc_data_fields(["245"], ["c"], exclude_subfields=True)

        # 242, all subfields
        f242 = self.get_marc_data_fields(["242"])

        # 240, all subfields
        f240 = self.get_marc_data_fields(["240"])

        return f245 + f242 + f240


    def get_types(self):
        """
        Legacy code verbatim
        :return:
        """
        return {
            "datafield": OrderedDict(
                [("AJ", ("Journal", "Text")),
                 ("AN", ("Newspaper", "Text")),
                 ("BI", ("Biography", "Text")),
                 ("BK", ("Book", "Text")),
                 ("CF", ("Computer File", "Interactive Resource")),
                 ("CR", ("CDROM", "Interactive Resource")),
                 ("CS", ("Software", "Software")),
                 ("DI", ("Dictionaries", "Text")),
                 ("DR", ("Directories", "Text")),
                 ("EN", ("Encyclopedias", "Text")),
                 ("HT", ("HathiTrust", None)),
                 ("MN", ("Maps-Atlas", "Image")),
                 ("MP", ("Map", "Image")),
                 ("MS", ("Musical Score", "Text")),
                 ("MU", ("Music", "Text")),
                 ("MV", ("Archive", "Collection")),
                 ("MW", ("Manuscript", "Text")),
                 ("MX", ("Mixed Material", "Collection")),
                 ("PP", ("Photograph/Pictorial Works", "Image")),
                 ("RC", ("Audio CD", "Sound")),
                 ("RL", ("Audio LP", "Sound")),
                 ("RM", ("Music", "Sound")),
                 ("RS", ("Spoken word", "Sound")),
                 ("RU", (None, "Sound")),
                 ("SE", ("Serial", "Text")),
                 ("SX", ("Serial", "Text")),
                 ("VB", ("Video (Blu-ray)", "Moving Image")),
                 ("VD", ("Video (DVD)", "Moving Image")),
                 ("VG", ("Video Games", "Moving Image")),
                 ("VH", ("Video (VHS)", "Moving Image")),
                 ("VL", ("Motion Picture", "Moving Image")),
                 ("VM", ("Visual Material", "Image")),
                 ("WM", ("Microform", "Text")),
                 ("XC", ("Conference", "Text")),
                 ("XS", ("Statistics", "Text"))]),
            "leader": OrderedDict(
                [("am", ("Book", "Text")),
                 ("asn", ("Newspapers", "Text")),
                 ("as", ("Serial", "Text")),
                 ("aa", ("Book", "Text")),
                 ("a(?![mcs])", ("Serial", "Text")),
                 ("[cd].*", ("Musical Score", "Text")),
                 ("t.*", ("Manuscript", "Text")),
                 ("[ef].*", ("Maps", "Image")),
                 ("g.[st]", ("Photograph/Pictorial Works", "Image")),
                 ("g.[cdfo]", ("Film/Video", "Moving Image")),
                 ("g.*", (None, "Image")),
                 ("k.*", ("Photograph/Pictorial Works", "Image")),
                 ("i.*", ("Nonmusic", "Sound")),
                 ("j.*", ("Music", "Sound")),
                 ("r.*", (None, "Physical object")),
                 ("p[cs].*", (None, "Collection")),
                 ("m.*", (None, "Interactive Resource")),
                 ("o.*", (None, "Collection"))])
        }


class UcbTindValidator(Validator):

    def setup(self):
        self.add_validatable_fields([
            {
                "field": "is_shown_by",
                "validations": [
                    Validator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            },
            {
                "field": "is_shown_at",
                "validations": [
                    Validator.str_match_ignore_url_protocol,
                    Validator.verify_type(str)
                ]
            }
        ])

class UcbTindVernacular(OaiVernacular):
    record_cls = UcbTindRecord
    validator = UcbTindValidator

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

        sickle_rec = models.Record(record_element)
        sickle_header = sickle_rec.header

        if sickle_header.deleted:
            return None

        record = {
            "datestamp": sickle_header.datestamp,
            "id": sickle_header.identifier,
            "request_url": request_url,
            "marc": parse_xml_to_array(StringIO(marc_collection_xml_full))[0]
        }

        return record
