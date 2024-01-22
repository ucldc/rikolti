from .marc_mapper import MarcRecord, MarcVernacular
from ..oai.oai_mapper import OaiVernacular

from sickle import models
from pymarc import parse_xml_to_array
from lxml import etree
from io import StringIO

from typing import Callable
from collections import OrderedDict

class UcbTindRecord(MarcRecord):
    def UCLDC_map(self):
        return {
            "calisphere-id": self.legacy_couch_db_id.split("--")[1],
            "isShownAt": self.get_marc_values(["856"], ["u"]),
            "isShownBy": self.get_marc_values(["856"], ["u"]),
            "language": self.get_marc_values(["041"], ["a"]),
            "date": self.get_marc_values(["260"], ["c"]),
            "publisher": self.get_marc_values(["260"], ["a", "b"]),
            "format": self.map_format,
            "extent": self.map_extent,
            "identifier": self.get_marc_values(["020", "022", "035"], ["a"]),
            "creator": self.get_marc_values(["100", "110", "111"]),
            "relation": self.map_relation,
            "description": self.map_description,
            "rights": self.get_marc_values(["506", "540"]),
            "temporal": self.get_marc_values(["648"]),
            "contributor": self.get_marc_values(["700", "710", "711", "720"]),
            "title": self.map_title,
            "spatial": self.map_spatial,
            "spec_type": self.map_spec_type,
            "subject": self.map_subject,
            "type": self.map_type,
        }

    def map_type(self):
        self.get_marc_leader("type_of_control") + self.get_marc_leader("bibliographic_level")

        print(self.get_marc_control_field("005", 1))
        print(self.get_marc_control_field("001", 3))

    def map_spec_type(self):
        pass

    def get_metadata_fields(self):
        return [str(i) for i in [600, 630, 650, 651] + list(range(610, 620)) + list(
            range(653, 659)) + list(range(690, 700))]

    def map_spatial(self) -> list:
        f651 = self.get_marc_data_fields(["651"], ["a"])

        return f651 + self.get_marc_data_fields(self.get_metadata_fields(), ["z"])

    def map_format(self) -> list:
        f3xx = self.get_marc_data_fields(["337", "338", "340"], ["a"]),

        return f3xx + self.get_marc_data_fields(self.get_metadata_fields(), ["v"])

    def map_subject(self) -> list:

        def get_delimiters(tag, code):
            """
            Returns the appropriate delimiter(s) based on the tag and code
            """
            if tag == "658":
                if code == "b":
                    return [":"]
                elif code == "c":
                    return [" [", "]"]
                elif code == "d":
                    return ["--"]
            elif ((tag == "653") or (int(tag) in range(690, 700)) or
                  (code == "b" and
                   tag in ("654", "655")) or (code in ("v", "x", "y", "z"))):
                return ["--"]
            elif (tag == "610"):
                if code == "b":
                    return [" "]
                else:
                    return ["--"]
            elif code == "d":
                return [", "]

            return [". "]

        def split_subject(value, tag, code):
            delimiters = get_delimiters(tag, code)

            if not code or code.isdigit():
                # Skip codes that are numeric
                return

            value = value.rstrip(", ")

            if value:
                delimiters = get_delimiters(tag, code)
                for delimiter in delimiters:
                    values = [delimiter.join(values)]
                    if delimiter != delimiters[-1]:
                        # Append an empty value for subsequent joins
                        values.append("")

            return values

        return self.get_marc_data_fields(self.get_metadata_fields(), process_value=split_subject)

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
        return self.get_marc_data_fields(["300"]) + self.get_marc_data_fields(["340"], ["b"])

    def map_title(self) -> list:
        # 245, all subfields except c
        f245 = self.get_marc_data_fields(["245"], ["c"], exclude_subfields=True)

        # 242, all subfields
        f242 = self.get_marc_data_fields(["242"])

        # 240, all subfields
        f240 = self.get_marc_data_fields(["240"])

        return f245 + f242 + f240

    def get_marc_control_field(self, field_tag: str, index: int = None) -> list:
        """
        Get MARC control field. Returns an empty string if:
            * Control field isn't set
            * No value exists at the requested index
        Otherwise it returns a value

        TODO: maybe need to accept slices in addition to ints for the index

        :param field_tag: Field tag to retrieve.
        :param index: A specific index to fetch
        :return: List of values for the control fields.
        """

        # Don't let any data tags sneak in! They have subfields.
        data_field_tag = field_tag if field_tag.isnumeric() and int(field_tag) < 100 else ""

        values = [v[0].value() for (k, v)
                  in self.get_marc_tag_value_map([data_field_tag]).items()
                  if len(v) > 0]

        if not values:
            return ""

        value = values[0]

        if index and len(value) > index + 1:
            return value[index]

        if index:
            return ""

        return value

    def get_marc_data_fields(self, field_tags: list, subfield_codes=[], **kwargs) -> list:
        """
        Get the values of specified subfields from given MARC fields.

        Set the `exclude_subfields` kwarg to exclude the specified subfield_codes.

        Set the `process_value` kwarg to pass the value through your own code to
        do transformations based on the field tag, code and value. See `map_subject` for
        an example.

        :param field_tags: A list of MARC fields.
        :param subfield_codes: A list of subfield codes to filter the values. If empty,
                               all subfields will be included.
        :return: A list of values of the specified subfields.
        """

        # Don't let any control tags sneak in! They don't have subfields.
        data_field_tags = [tag for tag in field_tags
                           if tag.isnumeric() and int(tag) > 99]

        def subfield_matches(check_code: str, subfield_codes: list, exclude_subfields: bool) -> bool:
            """
            :param check_code: The code to check against the subfield codes.
            :param subfield_codes: A list of subfield codes to include / exclude
            :param exclude_subfields: A boolean value indicating whether to exclude the
                                      specified subfield codes.
            :return: A boolean value indicating whether the check_code is included or
                    excluded based on the subfield_codes and exclude_subfields parameters.
            """
            if not subfield_codes:
                return True
            if exclude_subfields:
                return check_code not in subfield_codes
            else:
                return check_code in subfield_codes

        if "process_value" in kwargs and isinstance(kwargs["process_value"], Callable):
            process_value = kwargs["process_value"]
        else:
            process_value = None

        exclude_subfields = "exclude_subfields" in kwargs and kwargs[
            "exclude_subfields"]

        value_list = [process_value(value, field_tag, subfield[0])
                      if process_value else value
                      for (field_tag, matching_fields) in self.get_marc_tag_value_map(data_field_tags).items()
                      for matching_field in matching_fields
                      for subfield in list(matching_field.subfields_as_dict().items())
                      for value in subfield[1]
                      if subfield_matches(subfield[0], subfield_codes, exclude_subfields)]

        return value_list if isinstance(value_list, list) else []

    def get_marc_tag_value_map(self, field_tags: list) -> dict:
        """
        Get the specified MARC fields from the source_metadata, mapping by field tag

        :param field_tags: List of MARC fields to retrieve.
        :return: List of MARC fields from the source_metadata.
        """
        return {field_tag: self.source_metadata.get("marc").get_fields(field_tag) for
                field_tag in field_tags}

    def get_marc_leader(self, leader_key: str):
        """
        Retrieve the value of specified leader key from the MARC metadata.

        Couple things:
            * We're not accommodating passing a slice, which pymarc can handle should it be necessary
            * Both

        :param leader_key: The key of the leader field to retrieve.
        :type leader_key: str
        :return: The value of the specified leader key.
        :rtype: str or None
        """
        leader = self.source_metadata.get("marc").leader

        if str(leader_key).isnumeric():
            return leader[int(leader_key)]

        if hasattr(leader, leader_key):
            return leader.getattr(leader_key, "")

        return ""

    def get_type_mapping(self):
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
                [("am",         ("Book", "Text")),
                 ("asn",        ("Newspapers", "Text")),
                 ("as",         ("Serial", "Text")),
                 ("aa",         ("Book", "Text")),
                 ("a(?![mcs])", ("Serial", "Text")),
                 ("[cd].*",     ("Musical Score", "Text")),
                 ("t.*",        ("Manuscript", "Text")),
                 ("[ef].*",     ("Maps", "Image")),
                 ("g.[st]",     ("Photograph/Pictorial Works", "Image")),
                 ("g.[cdfo]",   ("Film/Video", "Moving Image")),
                 ("g.*",        (None, "Image")),
                 ("k.*",        ("Photograph/Pictorial Works", "Image")),
                 ("i.*",        ("Nonmusic", "Sound")),
                 ("j.*",        ("Music", "Sound")),
                 ("r.*",        (None, "Physical object")),
                 ("p[cs].*",    (None, "Collection")),
                 ("m.*",        (None, "Interactive Resource")),
                 ("o.*",        (None, "Collection"))])
        }

class UcbTindVernacular(OaiVernacular):
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
