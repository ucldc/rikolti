from io import StringIO
from itertools import chain
import re
from typing import Callable, Optional

from lxml import etree
from pymarc import parse_xml_to_array
from sickle import models

from ..mapper import Record, Vernacular, Validator


class UcbTindRecord(Record):
    def UCLDC_map(self):
        import json
        print(f"{self.source_metadata=}")
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
            "creator": self.get_marc_data_fields(["700"], ["a"]),
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

    def get_marc_control_field(self, field_tag: str, index: int = None) -> list:
        """

        See: https://www.loc.gov/marc/bibliographic/bd00x.html

        Get MARC control field. Returns an empty string if:
            * Control field isn't set
            * No value exists at the requested index
        Otherwise it returns a value

        :param field_tag: Field tag to retrieve.
        :param index: A specific index to fetch
        :return: List of values for the control fields.
        """

        # Don't let any data tags sneak in! They have subfields.
        data_field_tag = field_tag if field_tag.isnumeric() and int(
            field_tag) < 100 else ""

        values = [v[0].value() for (k, v)
                  in self.marc_tags_as_dict([data_field_tag]).items()
                  if len(v) > 0]

        if not values:
            return ""

        value = values[0]

        if index and len(value) > index + 1:
            return value[index]

        if index:
            return ""

        return value

    def get_marc_data_fields(self, field_tags: list, subfield_codes=[], recurse=True,
                             **kwargs) -> list:
        """
        TODO: Variable name meaning becomes quite fuzzy in the heart of this
              function. Most variables could stand to be renamed.

        `Data fields` is not a specific term in MARC. This function really will accept
        any field tags. See https://www.loc.gov/marc/bibliographic/ for all fields.

        In most cases, this returns the Cartesian product of the provided `field_tags`
        and `subfield codes`. If `recurse` is true, it will augment to include values
        from field 880. Note the special handling of code `6`.

        Set the `exclude_subfields` kwarg to exclude the specified subfield_codes.

        Set the `process_value` kwarg to pass the value through your own code to
        do transformations based on the field tag, code and value. There isn't an
        example of this in use currently, but it could be useful for debugging or
        for context-based transformations.

        :param recurse: Indicates whether alternate graphic representations (field 880)
                        should be sought. This is used here to prevent infinite loops
                        when this function is called to get field 880. It would also be
                        possible (and maybe preferable) to remove this argument and set
                        a `recurse` variable to false if "880" is included among
                        `field_tags`.
        :param field_tags: A list of MARC fields.
        :param subfield_codes: A list of subfield codes to filter the values. If empty,
                               all subfields will be included.
        :return: A list of values of the specified subfields.
        """
        def subfield_matches(check_code: str, subfield_codes: list,
                             exclude_subfields: bool) -> bool:
            """
            :param check_code: The code to check against the subfield codes.
            :param subfield_codes: A list of subfield codes to include / exclude
            :param exclude_subfields: A boolean value indicating whether to exclude the
                                      specified subfield codes.
            :return: A boolean value indicating whether the check_code is included or
                    excluded based on the subfield_codes and exclude_subfields parameters.
            """

            # Always exclude subfield 6 (Linkage,
            # see: https://www.loc.gov/marc/bibliographic/ecbdcntf.html) unless it is
            # explicitly listed. Not excluding this was producing results that
            # were not expected. Note the explicit inclusion of 6 in
            # `get_alternate_graphic_representation()`, below.
            if check_code == "6" and "6" not in subfield_codes:
                return False
            if not subfield_codes:
                return True
            if exclude_subfields:
                return check_code not in subfield_codes
            else:
                return check_code in subfield_codes

        def get_alternate_graphic_representation(tag: str, code: str, index: int,
                                                 recurse=True) -> list:
            """
            This is where field 880 is handled.
            See: https://www.loc.gov/marc/bibliographic/bd880.html

            :param tag:
            :param code:
            :param index:
            :param recurse:
            :return:
            """
            if not recurse:
                return []

            subfield_6 = self.get_marc_data_fields([tag], ["6"], False)
            if not subfield_6 or index >= len(subfield_6):
                return []

            match = re.match(r"^880\-([0-9]+)$", subfield_6[index])
            if not match:
                return []

            all_880 = self.marc_tags_as_dict(["880"])["880"]
            index_880 = int(match.group(1)) - 1  # 880 indices start at 1

            if not all_880 or index_880 >= len(all_880):
                return []

            field = all_880[index_880]
            subfields = field.subfields_as_dict()

            if code not in subfields:
                return []

            return subfields[code]

        if "process_value" in kwargs and isinstance(kwargs["process_value"], Callable):
            process_value = kwargs["process_value"]
        else:
            process_value = None

        exclude_subfields = "exclude_subfields" in kwargs and kwargs[
            "exclude_subfields"]

        # Do we want process_value to have access to the 880 field values as well?
        # If so, call process_value with value + the output of
        # get_alternate_graphic_representation
        value_list = [[(process_value(value, field_tag, subfield[0])
                      if process_value else value)] +
                      get_alternate_graphic_representation(field_tag, subfield[0], field_index, recurse)

                      # Iterate the fields that have tags matching those requested
                      for (field_tag, matching_fields) in
                      self.marc_tags_as_dict(field_tags).items()

                      # Iterate the individual matches, tracking order in index
                      for field_index, matching_field in enumerate(matching_fields)

                      # Iterate the subfield codes in those fields
                      for subfield in list(matching_field.subfields_as_dict().items())

                      # Iterate the values in those subfields
                      for value in subfield[1]
                      if

                      # Ensure we're including only requested subfields
                      subfield_matches(subfield[0], subfield_codes, exclude_subfields)]

        # Dedupe the output
        deduped_values = []
        [deduped_values.append(value) for value in value_list
         if value not in deduped_values]

        # Flatten the output
        flattened_values = list(chain.from_iterable(deduped_values)) if (
            isinstance(deduped_values, list)) else []

        return flattened_values

    def marc_tags_as_dict(self, field_tags: list) -> dict:
        """
        Get the specified MARC fields from the source_metadata, mapping by field tag

        :param field_tags: List of MARC fields to retrieve.
        :return: List of MARC fields from the source_metadata.
        """
        return {field_tag: self.source_metadata.get('marc').get_fields(field_tag) for
                field_tag in field_tags}

    def get_marc_leader(self, leader_key: str):
        """
        Retrieve the value of specified leader key from the MARC metadata.
        See: https://www.loc.gov/marc/bibliographic/bdleader.html

        We're not accommodating passing a slice, which pymarc can handle should it be necessary

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
            
    def map_is_shown_at(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return "https://digicoll.lib.berkeley.edu/record/" + field_001

    def map_is_shown_by(self):
        field_001 = self.get_marc_control_field("001")
        if field_001:
            return ("https://digicoll.lib.berkeley.edu/nanna/thumbnail/v2/" +
                    field_001 + "?redirect=1")

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

        values = f651 + self.get_marc_data_fields(self.get_metadata_fields(), ["z"])

        # Stripping off trailing period
        return [value[0:-1] if value[-1] == "." else value for value in values]

    def map_subject(self) -> list:
        fields = self.get_metadata_fields()
        return [{"name": s} for s in
                self.get_marc_data_fields(fields, ["2"], exclude_subfields=True)]

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
        return [", ".join(
            self.get_marc_data_fields(["300"]) + self.get_marc_data_fields(["340"],
                                                                           ["b"]))]

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
        for re in record_elements:
            record = self._process_record(re, request_url)
            # sickle_rec = models.Record(re)
            # sickle_header = sickle_rec.header
            # if sickle_header.deleted:
            #     continue

            # record = self.strip_metadata(sickle_rec.metadata)
            # record["datestamp"] = sickle_header.datestamp
            # record["id"] = sickle_header.identifier
            # record["request_url"] = request_url
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

    def _process_record(self,
                        record_element: etree.ElementBase,
                        request_url: Optional[str]) -> Optional[dict]:
        """
        Process a record element and extract relevant information.

        :param record_element: Element representing a single record.
        :param request_url: The URL of the request.
        :return: A dictionary containing the extracted information from the record.
        """
        sickle_rec = models.Record(record_element)
        sickle_header = sickle_rec.header
        if sickle_header.deleted:
            return None

        marc_record_element = record_element.find(".//marc:record", namespaces={
            "marc": "http://www.loc.gov/MARC21/slim"})
        marc_record_string = etree.tostring(marc_record_element,
                                            encoding="utf-8").decode("utf-8")

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

        return record