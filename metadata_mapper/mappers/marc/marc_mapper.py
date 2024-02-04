from ..oai.oai_mapper import OaiVernacular
from ..mapper import Record

from typing import Callable
import re
from itertools import chain

class MarcRecord(Record):
    def UCLDC_map(self):
        return {
        }

    def get_marc_control_field(self, field_tag: str, index: int = None) -> list:
        """
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

    def get_marc_data_fields(self, field_tags: list, subfield_codes=[], recurse=True,
                             **kwargs) -> list:
        """
        TODO: Variable name meaning becomes quite fuzzy in the heart of this
              function. Most variables could stand to be renamed.

        Get the values of specified subfields from given MARC fields. This allows
        control fields too.

        Set the `exclude_subfields` kwarg to exclude the specified subfield_codes.

        Set the `process_value` kwarg to pass the value through your own code to
        do transformations based on the field tag, code and value. See `map_subject` for
        an example.

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

            # Always exclude subfield 6 unless it is explicitly listed
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
            This is where field 880 is handled
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

            all_880 = self.get_marc_tag_value_map(["880"])["880"]
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
                      self.get_marc_tag_value_map(field_tags).items()

                      # Iterate the individual matches, tracking order in index
                      for field_index, matching_field in enumerate(matching_fields)

                      # Iterate the subfield codes in those fields
                      for subfield in list(matching_field.subfields_as_dict().items())

                      # Iterate the values in those subfields
                      for value in subfield[1]
                      if

                      # Ensure we're including only requested subfields
                      subfield_matches(subfield[0], subfield_codes, exclude_subfields)]

        # Flatten the output
        values = list(chain.from_iterable(value_list)) if isinstance(value_list, list) else []

        # Dedupe the output
        deduped_values = []
        [deduped_values.append(value) for value in values
         if value not in deduped_values]

        return deduped_values

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
