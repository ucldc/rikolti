from ..oai.oai_mapper import OaiVernacular
from ..mapper import Record

from typing import Callable

class MarcRecord(Record):
    def UCLDC_map(self):
        return {

        }

    def map_type(self):
        value = []
        for types in self.get_matching_types():
            value.append(types[1])

        return value

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
        data_field_tag = field_tag if field_tag.isnumeric() and int(
            field_tag) < 100 else ""

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

    def get_marc_data_fields(self, field_tags: list, subfield_codes=[],
                             **kwargs) -> list:
        """
        Get the values of specified subfields from given MARC fields. This allows control fields too.

        Set the `exclude_subfields` kwarg to exclude the specified subfield_codes.

        Set the `process_value` kwarg to pass the value through your own code to
        do transformations based on the field tag, code and value. See `map_subject` for
        an example.

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
                      for (field_tag, matching_fields) in
                      self.get_marc_tag_value_map(field_tags).items()
                      for matching_field in matching_fields
                      for subfield in list(matching_field.subfields_as_dict().items())
                      for value in subfield[1]
                      if
                      subfield_matches(subfield[0], subfield_codes, exclude_subfields)]

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



class MarcVernacular(OaiVernacular):
    pass
