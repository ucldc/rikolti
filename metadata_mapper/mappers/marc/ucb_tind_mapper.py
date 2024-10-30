from io import StringIO
from typing import Any

from lxml import etree
from pymarc import parse_xml_to_array
from sickle import models

from ..mapper import Record, Vernacular, Validator


class UcbTindRecord(Record):

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


    def get_880_fields(self) -> dict:
        '''
        Returns a dict of 880 fields for the record, e.g.:

        marc_880_fields = {
            '01': [
                Subfield(code='6', value='700-01'), 
                Subfield(code='a', value='雪谷.')
            ], 
            '02': [
                Subfield(code='6', value='245-02'),
                Subfield(code='a', value='保壽軒御茶銘雙六 ')
            ],
            '03': [
                Subfield(code='6', value='246-03'),
                Subfield(code='a', value='保壽軒')
            ], 
            '04': [
                Subfield(code='6', value='260-04'), 
                Subfield(code='a', value='横濱 '), 
                Subfield(code='a', value='東京 '), 
                Subfield(code='b', value='桝本保五郎'), 
                Subfield(code='c', value='[between 1868 and 1912]')
            ]
        }
        '''
        marc_880_fields = {}
        marc_data = self.source_metadata.get("marc")

        for field in marc_data.get_fields("880"):
            for subfield in field.subfields:
                if self.subfield_matches(subfield.code, ['6'], False):
                    field_880_key = subfield.value.split('-')[1]

            marc_880_fields[field_880_key] = field.subfields

        return marc_880_fields


    def subfield_matches(self, check_code: str, subfield_codes: list,
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
            # were not expected.
            if check_code == "6" and "6" not in subfield_codes:
                return False
            if not subfield_codes:
                return True
            if exclude_subfields:
                return check_code not in subfield_codes
            else:
                return check_code in subfield_codes


    def marc_tags_as_dict(self, field_tags: list) -> dict:
        """
        Get the specified MARC fields from the source_metadata, mapping by field tag

        :param field_tags: List of MARC fields to retrieve.
        :return: List of MARC fields from the source_metadata.
        """
        return {field_tag: self.source_metadata.get("marc").get_fields(field_tag) for
                field_tag in field_tags}


    def get_marc_data_fields(self, field_tags: list, subfield_codes=[], get_880_values=True,
                             exclude_subfields=False) -> list:
        """
        In most cases, this returns the Cartesian product of the provided `field_tags`
        and `subfield codes`. If `get_880_values` is true, it will augment to include values
        from field 880. Note the special handling of code `6`.

        Set the `exclude_subfields` kwarg to exclude the specified subfield_codes.

        :param field_tags: A list of MARC fields.
        :param subfield_codes: A list of subfield codes to include / exclude
        :param get_880_values: Indicates whether alternate graphic representations
                               (field 880) should be sought.
        :param exclude_subfields: A boolean value indicating whether to exclude the
                                  specified subfield codes.
        :return: A list of values of the specified subfields.
        """
        values = []
        for tag in field_tags:
            for marc_field in self.source_metadata.get("marc").get_fields(tag):
                field_880_key = None
                # get 880 field key so we can look up corresponding 880 field
                if get_880_values:
                    for subfield in marc_field.subfields:
                        if self.subfield_matches(subfield.code, ['6'], False):
                            field_880_key = subfield.value.split('-')[1]

                # get subfield values, plus any corresponding 880 values
                for index, subfield in enumerate(marc_field.subfields):
                    if self.subfield_matches(subfield.code, subfield_codes, exclude_subfields):
                        values.append(subfield.value)
                        if field_880_key:
                            values.append(
                                self.marc_880_fields[field_880_key][index].value
                            )

        return values


    def get_marc_leader(self, leader_key: str):
        """
        Note: This is a stub. Leaving here in case it is needed in other marc mappers.

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
