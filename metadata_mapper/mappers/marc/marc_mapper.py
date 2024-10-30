from ..mapper import Record

class MarcRecord(Record):
    def UCLDC_map(self):
        return {
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

