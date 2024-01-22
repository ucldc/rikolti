from .marc_mapper import MarcRecord, MarcVernacular
from ..oai.oai_mapper import OaiVernacular

from sickle import models
from pymarc import parse_xml_to_array
from lxml import etree
from io import StringIO


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
            "subject": self.map_subject
        }

    def get_metadata_fields(self):
        return [str(i) for i in [600, 630, 650, 651] + list(range(610, 620)) + list(
            range(653, 659)) + list(range(690, 700))]

    def map_spatial(self) -> list:
        f651 = self.get_marc_values(["651"], ["a"])

        return f651 + self.get_marc_values(self.get_metadata_fields(), ["z"])

    def map_format(self) -> list:
        f3xx = self.get_marc_values(["337", "338", "340"], ["a"]),

        return f3xx + self.get_marc_values(self.get_metadata_fields(), ["v"])

    def map_subject(self) -> list:
        return self.get_marc_values(self.get_metadata_fields())

    def map_temporal(self) -> list:
        f648 = self.get_marc_values(["648"])

        return f648 + self.get_marc_values(self.get_metadata_fields(), ["y"])

    def map_format(self) -> list:
        return self.get_marc_values(self.get_metadata_fields(), ["v"])

    def map_description(self) -> list:
        field_range = [str(i) for i in range(500, 600) if i != 538]

        return self.get_marc_values(field_range)

    def map_relation(self) -> list:
        field_range = [str(i) for i in range(760, 788)]  # Up to 787

        self.get_marc_values(field_range)

    def map_identifier(self) -> list:
        f050 = self.get_marc_values(["050"], ["a", "b"])

        return f050 + self.get_marc_values(["020", "022", "035"], ["a"])

    def map_extent(self) -> list:
        """
        Retrieves the extent values from MARC field 300 and 340.

        :return: A list of extent values.
        """
        return self.get_marc_values(["300"]) + self.get_marc_values(["340"], ["b"])

    def map_title(self) -> list:
        # 245, all subfields except c
        f245 = self.get_marc_values(["245"], ["c"], exclude_subfields=True)

        # 242, all subfields
        f242 = self.get_marc_values(["242"])

        # 240, all subfields
        f240 = self.get_marc_values(["240"])

        return f245 + f242 + f240

    def get_marc_values(self, field_tags: list, subfield_codes=[], **kwargs) -> list:
        """
        Get the values of specified subfields from given MARC fields.

        :param field_tags: A list of MARC fields.
        :param subfield_codes: A list of subfield codes to filter the values. If empty, all subfields will be included.
        :return: A list of values of the specified subfields.
        """

        def subfield_matches(check_code: str, subfield_codes: list, exclude_subfields: bool) -> bool:
            """
            :param check_code: The code to check against the subfield codes.
            :param subfield_codes: A list of subfield codes to include / exclude
            :param exclude_subfields: A boolean value indicating whether to exclude the specified subfield codes.
            :return: A boolean value indicating whether the check_code is included or excluded based on the subfield_codes and exclude_subfields parameters.
            """
            if not subfield_codes:
                return True
            if exclude_subfields:
                return check_code not in subfield_codes
            else:
                return check_code in subfield_codes

        exclude_subfields = "exclude_subfields" in kwargs and kwargs[
            'exclude_subfields']

        matching_fields = [field for field in self.get_marc_fields(field_tags)]

        value_list = [value
                      for matching_field in matching_fields
                      for subfield in list(matching_field.subfields_as_dict().items())
                      for value in subfield[1]
                      if subfield_matches(subfield[0], subfield_codes, exclude_subfields)]

        return value_list if isinstance(value_list, list) else []

    def get_marc_fields(self, field_tags: list) -> list:
        """
        Get the specified MARC fields from the source_metadata.

        :param field_tags: List of MARC fields to retrieve.
        :return: List of MARC fields from the source_metadata.
        """

        return [f for f in self.source_metadata.get("marc").get_fields(*field_tags)]


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
