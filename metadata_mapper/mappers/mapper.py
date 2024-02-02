import hashlib
import itertools
import json
import os
import re
from abc import ABC
from datetime import date, datetime
from datetime import timezone
from typing import Any, Callable, Optional

from markupsafe import Markup

from ..utilities import returns_callable
from ..validator.validation_log import ValidationLog  # noqa: F401
from ..validator.validator import Validator
from . import constants
from .iso639_1 import iso_639_1
from .iso639_3 import iso_639_3, language_regexes, wb_language_regexes


class Vernacular(ABC, object):
    def __init__(self, collection_id: int, page_filename: str) -> None:
        self.collection_id = collection_id
        self.page_filename = page_filename

    def get_records(self, records):
        return [
            self.record_cls(self.collection_id, record)
            for record in records if not self.skip(record)
        ]

    def skip(self, record):
        return False


class Record(ABC, object):

    validator = Validator

    def __init__(self, collection_id: int, record: dict[str, Any]):
        self.mapped_data = None
        self.collection_id: int = collection_id
        self.source_metadata: dict = record
        # TODO: pre_mapped_data is a stop gap to accomodate
        # pre-mapper enrichments, should probably squash this.
        self.pre_mapped_data = {}
        self.enrichment_report = []

    def to_dict(self) -> dict[str, Any]:
        return self.mapped_data

    def to_UCLDC(self) -> dict[str, Any]:
        """
        Maps source metadata to UCLDC format, saving result to
        self.mapped_data.

        Returns: dict
        """
        supermaps = [
            super(c, self).UCLDC_map()
            for c in list(reversed(type(self).__mro__))
            if hasattr(super(c, self), "UCLDC_map")
        ]

        mapped_data = {}
        for supermap in supermaps:
            mapped_data = {**mapped_data, **supermap}
        mapped_data = {**mapped_data, **self.UCLDC_map()}


        # Mapped value may be a function or lambda
        self.mapped_data = {k: v() if isinstance(v, Callable) else v for (k, v)
                            in mapped_data.items()}

        return self.mapped_data

    def UCLDC_map(self) -> dict:
        """
        Defines mappings from source metadata to UCDLC that are specific
        to this implementation.

        All dicts returned by this method up the ancestor chain
        are merged together to produce a final result.
        """
        return {}

    # Validation
    def validate(self, comparison_data: dict) -> ValidationLog:
        return self.validator.validate(self.mapped_metadata, comparison_data)

    # Mapper Helpers
    @returns_callable
    def collate_subfield(self, field, subfield):
        return [f[subfield] for f in self.source_metadata.get(field, [])]

    @returns_callable
    def collate_fields(self, fieldlist):
        ''' collate multiple field values into a single list '''
        collated = []
        for field in fieldlist:
            value = self.source_metadata.get(field)
            if value:
                if isinstance(value, str):
                    collated.append(value)
                else:
                    collated.extend(value)

        return collated

    @returns_callable
    def first_string_in_field(self, field):
        """
        Fetches a field value from source_metadata, and returns it if it's a 
        string, or the first item if it's a list
        """
        value = self.source_metadata.get(field)
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return value[0]

    @returns_callable
    def split_and_flatten(self, field):
        """
        Given a list of strings or nested lists, splits the values
        on the split string then flattens
        """
        values = self.source_metadata.get(field)

        if not values:
            return

        split_values = [c.split(';') for c in filter(None, values)]

        return list([s.strip() for s in itertools.chain.from_iterable(split_values)])

    # Enrichments
    # The enrichment chain is a dpla construction that we are porting to Rikolti
    # The enrichment chain is implemented as methods on Rikolti's Record() class

    def enrich(self, enrichment_function_name: str, **kwargs):
        func: Callable = getattr(self, enrichment_function_name)
        try:
            return func(**kwargs)
        except Exception as e:
            print(f"ENRICHMENT ERROR: {str(kwargs)}")
            raise e

    def select_id(self, prop: list[str]):
        """
        called with the following parameters:
        278 times:  prop=["uid"]
        1429 tiems: prop=["id"]
        3 times:    prop=["PID"]
        7 times:    prop=["metadata/identifier"]
        3 times:    prop=["identifier"]
        """
        if len(prop) > 1:
            raise Exception("select_id only accepts one property")
        if not isinstance(prop, list):
            raise Exception("select_id only accepts a list of length 1")
        prop = prop[0]

        # original dpla_ingestion/lib/akamod/select-id.py
        # does two things not implemented here:
        # 1. a tree traversal get instead of a simple .get(prop)
        # 2. handles if the id_handle is not just a simple string
        # not sure if we need to do either of these things, so
        # making a simple implementation for now

        id_handle = self.source_metadata.get(prop)
        lname = id_handle.strip().replace(" ", "__")

        self.legacy_couch_db_id = (f"{self.collection_id}--{lname}")
        return self

    def select_oac_id(self):
        """
        called 574 times with no parameters
        only ever called prior to mapping, so update self.pre_mapped_data
        """
        id_values = self.source_metadata.get("identifier")
        if not isinstance(id_values, list):
            id_values = [id_values]

        ark_ids = [v.get('text') for v in id_values
                   if v.get('text').startswith("http://ark.cdlib.org/ark:")]

        calisphere_id = None
        if ark_ids:
            calisphere_id = ark_ids[0]
        else:
            calisphere_id = id_values[0]

        self.pre_mapped_data["id"] = f"{self.collection_id}--{calisphere_id}"
        self.pre_mapped_data["calisphere-id"] = f"{calisphere_id}"
        self.pre_mapped_data["isShownAt"] = calisphere_id
        self.pre_mapped_data["isShownBy"] = f"{calisphere_id}/thumbnail"
        self.legacy_couch_db_id = (f"{self.collection_id}--{calisphere_id}")

        return self

    def select_cmis_atom_id(self):
        """
        called 21 times with no parameters
        """
        calisphere_id = None
        # the json representation is crazy
        core_namespace = "{http://docs.oasis-open.org/ns/cmis/core/200908/}"
        rest_namespace = "{http://docs.oasis-open.org/ns/cmis/restatom/200908/}"
        id_values = (
            self.mapped_data
            .get("{http://www.w3.org/2005/Atom}entry", {})
            .get(f"{rest_namespace}object", {})
            .get(f"{core_namespace}properties", {})
            .get(f"{core_namespace}propertyId", None)
        )
        for id_value in id_values:
            if id_value.get("@propertyDefinitionId", '') == "cmis:objectId":
                calisphere_id = id_value[f"{core_namespace}value"]["$"]

        if not calisphere_id:
            raise ValueError("Couldn't find property to extract id")

        calisphere_id = calisphere_id.split('|')[1]

        self.legacy_couch_db_id = f"{self.collection_id}--{calisphere_id}"
        return self

    def select_preservica_id(self):
        calisphere_id = self.mapped_data.get("preservica_id", {}).get('$')
        self.legacy_couch_db_id = f"{self.collection_id}--{calisphere_id}"
        return self

    def required_values_from_collection_registry(
            self, collection, field, mode=None):
        """
        called with the following parameters:
        2308 times: field=["rights"],   mode=["fill"]
        1961 times: field=["type"],     mode=["fill"]
        2035 times: field=["title"],    mode=["fill"]
        373 times:  field=["type"],     mode=["overwrite"]
        34 times:   field["rights"],    mode=["overwrite"]
        """
        field = field[0]
        mode = mode[0] if mode else None
        field_value = None

        if field == "rights":
            rights = [
                constants.rights_status.get(collection.get('rights_status')),
                collection.get('rights_statement')
            ]
            rights = [r for r in rights if r]

            if not rights:
                rights = [constants.rights_statement_default]
            field_value = [r.strip() for r in rights]

        if field == "type":
            field_value = constants.dcmi_types.get(
                collection.get('dcmi_type'), None)
            field_value = [field_value] if field_value else None

        if field == "title":
            field_value = ["Title unknown"]

        if not self.mapped_data or not isinstance(self.mapped_data, dict):
            raise ValueError("no mapped metadata record")

        if field_value:
            if mode == "overwrite":
                self.mapped_data[field] = field_value
            elif mode == "append":
                if field in self.mapped_data:
                    self.mapped_data[field] += (field_value)
                else:
                    self.mapped_data[field] = field_value
            else:  # default is fill if empty
                if not self.mapped_data.get(field):
                    self.mapped_data[field] = field_value

        # not sure what this is about
        # if not exists(data, "@context"):
        # self.mapped_data["@context"] = "http://dp.la/api/items/context"
        return self

    def shred(self, prop, delim=";"):
        """
        Based on DPLA-Ingestion service that accepts a JSON document and
        "shreds" the value of the field named by the "prop" parameter

        Splits values by delimeter; handles some complex edge cases beyond what
        split() expects. For example:
            ["a,b,c", "d,e,f"] -> ["a","b","c","d","e","f"]
            'a,b(,c)' -> ['a', 'b(,c)']
        Duplicate values are removed.

        called with the following parameters:
        2,078 times:    prop=["sourceResource/spatial"],       delim = ["--"]
        1,021 times:    prop=["sourceResource/subject/name"]
        1,021 times:    prop=["sourceResource/creator"]
        1,021 times:    prop=["sourceResource/type"]
        5 times:        prop=["sourceResource/description"],   delim=["<br>"]
        """
        field = prop[0].split('/')[1:][0]  # remove sourceResource
        delim = delim[0]

        # TODO: this is a way to get around cases where a key with name <field>
        # it present in self.mapped_data, but the value is None. Should get rid
        # of these keys in the first place.
        if field not in self.mapped_data or not self.mapped_data[field]:
            return self

        value = self.mapped_data[field]
        if isinstance(value, list):
            try:
                value = delim.join(value)
                value = value.replace(f"{delim}{delim}", delim)
            except Exception as e:
                self.enrichment_report.append(
                    f"[shred]: Can't join {field} list {value} with {delim}, {e}"
                )
        if delim not in value:
            return self
        shredded = re.split(re.escape(delim), value)

        shredded = [s.strip() for s in shredded if s.strip()]
        result = []
        for s in shredded:
            if s not in result:
                result.append(s)
        self.mapped_data[field] = result

        return self

    def copy_prop(self, prop, to_prop, no_overwrite=None, skip_if_exists=[False]):
        """
        no_overwrite is specified in one of the enrichment chain items, but is
        not implemented in the dpla-ingestion code.

        sourceResource%4Fpublisher is specified in one of the enrichment chain
        items, probably a typo; not sure what happens in the existing
        enrichment chain (does it just skip this enrichment?)

        we don't really quite have 'originalRecord', 'provider', or
        'sourceResource' in the Rikolti data model. Currently splitting on '/'
        and copying from whatever is after the '/', but will need to keep an
        eye on this. not sure what 'dataProvider' is, either or if we use it

        called with the following parameters:
        1105 times: prop=["originalRecord/collection"]
                    to_prop=["dataProvider"]
        1000 times: prop=["provider/name"],
                    to_prop=["dataProvider"],
        675 times:  prop=["sourceResource/publisher"],
                    to_prop=["dataProvider"]
                    skip_if_exists=["True"]
        549 times:  prop=["provider/name"],
                    to_prop=["dataProvider"],
                    no_overwrite=["True"]
                    # no_overwrite not implemented in dpla-ingestion
        440 times:  prop=["provider/name"],
                    to_prop=["dataProvider"],
                    skip_if_exists=["true"]
        293 times:  prop=["sourceResource%4Fpublisher"],
                    to_prop=["dataProvider"]
        86 times:   prop=["provider/name"],
                    to_prop=["dataProvider"]
        11 times:   prop=["originalRecord/type"],
                    to_prop=["sourceResource/format"]
        1 times:    prop=["sourceResource/spatial"],
                    to_prop=["sourceResource/temporal"],
                    skip_if_exists=["true"]
        1 times:    prop=["sourceResource/description"],
                    to_prop=["sourceResource/title"]
        """

        src = prop[0].split('/')[-1]
        dest = to_prop[0].split('/')[-1]
        skip_if_exists = bool(skip_if_exists[0] in ['True', 'true'])

        if ((dest in self.mapped_data and skip_if_exists) or
                (src not in self.mapped_data)):
            # dpla-ingestion code "passes" here, but I'm not sure what that
            # means - does it skip this enrichment or remove this record?
            return self

        src_val = self.mapped_data.get(src)
        dest_val = self.mapped_data.get(dest, [])
        if (
                (not (isinstance(src_val, list) or isinstance(src_val, str))) or
                (not (isinstance(dest_val, list) or isinstance(dest_val, str)))
        ):
            self.enrichment_report.append(
                f"[copy_prop]: Prop {src} is {type(src_val)} and prop {dest} "
                f"is {type(dest_val)} - not a string/list"
            )
            return self

        if isinstance(src_val, str):
            src_val = [src_val]
        if isinstance(dest_val, str):
            dest_val = [dest_val]

        self.mapped_data[dest] = dest_val + src_val
        return self

    def move_date_values(self, prop, dest="temporal"):
        """
        this is a fairly straight copy of the dpla-ingestion code
        dest is sourceResource/temporal in the dpla-ingestion code, but we
        don't have a sourceResource in the Rikolti data model. dest is also
        previously called 'to_prop'.

        called with the following parameters:
        # 2079 times: prop=["sourceResource/subject"]
        # 2079 times: prop=["sourceResource/spatial"]
        """
        src = prop[0].split('/')[-1]  # remove sourceResource
        # TODO: this is a way to get around cases where a key with name <src>
        # is present in self.mapped_data, but the value is None. Should get rid
        # of None value keys in self.mapped_data
        src_values = self.mapped_data.get(src)
        if not src_values:
            return self
        if isinstance(src_values, str):
            src_values = [src_values]

        # TODO: this is the same issue as above, where a key with name <dest>
        # is present in self.mapped_data, but the value is None. Should get rid
        # of None value keys in self.mapped_data
        dest_values = self.mapped_data.get(dest, [])
        if not dest_values:
            dest_values = []
        if isinstance(dest_values, str):
            dest_values = [dest_values]

        remove = []
        for value in src_values:
            if not isinstance(value, str):
                continue
            cleaned_value = re.sub(r"[\(\)\.\?]", "", value)
            cleaned_value = cleaned_value.strip()
            for pattern in constants.move_date_value_reg_search:
                matches = re.compile(pattern, re.I).findall(cleaned_value)
                if (len(matches) == 1 and
                        (not re.sub(matches[0], "", cleaned_value).strip())):
                    if matches[0] not in dest_values:
                        dest_values.append(matches[0])
                    remove.append(value)
                    break

        if dest_values:
            self.mapped_data[dest] = dest_values
            if len(src_values) == len(remove):
                del self.mapped_data[src]
            else:
                self.mapped_data[src] = [
                    v for v in src_values if v not in remove]

        return self

    def lookup(self, prop, target, substitution, inverse=[False]):
        """
        dpla-ingestion code has a delnonexisting parameter that is not used by
        our enrichment chain, so I've not implemented it here.

        since we don't have a sourceResource data structure, I'm not actually
        sure where these properties will exist in our new data model - will
        they still exist in some sort of nested `language: {name: "English"}`
        structure?

        dpla-ingestion codebase uses function `the_same_beginning` to check
        that the prop and target fields have the same beginning before doing
        the lookup, looks like it matters because you provide 1 path to both
        the source field and the destination field (in function `convert`).

        dpla-ingestion codebase uses function `find_conversion_dictionary` to
        look in akara.conf for 'lookup_mapping' and select, for example:
        `dict_name = lookup_mapping["iso639_3"]`. Using dict_name, it then
        looks in `globals()[dict_name]` for the actual lookup dictionary. I
        can't seem to find iso639_3 in the dpla-ingestion codebase.
        scdl_fix_format is right in the lookup.py file, though.

        2031 times: prop=["sourceResource/language/name"],
                    target=["sourceResource/language/name"],
                    substitution=["iso639_3"]
        1946 times: prop=["sourceResource/language/name"],
                    target=["sourceResource/language/iso639_3"],
                    substitution=["iso639_3"],
                    inverse=["True"]
        3 times:    prop=["sourceResource/format"],
                    target=["sourceResource/format"],
                    substitution=["scdl_fix_format"]
        """
        src = prop[0].split('/')[1:]  # remove sourceResource
        dest = target[0].split('/')[1:]  # remove sourceResource
        substitution = substitution[0]
        inverse = bool(inverse[0] in ['True', 'true'])

        # TODO: this won't actually work for deeply nested fields

        src_values = self.mapped_data
        if not src_values:
            return self

        for src_field in src:
            if not src_field:
                continue

            if not isinstance(src_values, dict) or src_field not in src_values:
                self.enrichment_report.append(
                    f"[lookup]: Source field {src} not in record"
                )
                return self
            src_values = src_values.get(src_field)

        if isinstance(src_values, str):
            src_values = [src_values]

        substitution_dict = {}
        if substitution == 'scdl_fix_format':
            substitution_dict = constants.scdl_fix_format
        if inverse:
            substitution_dict = {v: k for k, v in substitution_dict.items()}

        dest_values = [substitution_dict.get(v, v) for v in src_values]
        self.mapped_data[dest] = dest_values

        return self

    def enrich_location(self, prop=["sourceResource/spatial"]):
        """
        the enrich_location.py file in dpla-ingestion includes the functions:
        `get_isostate` and `from_abbrev`, as well as the constants `STATES` and
        `ABBREV`; oddly, though, the `enrich_location` enrichment service
        doesn't actually use any of these functions or constants. I've not
        implemented them here, choosing instead only to implement the function
        `enrichlocation`, which seemingly cleans whitespace around semicolons
        and then makes a dictionary? This code is so convoluted, I'm not
        implementing it until I have proper sample data to test it against.

        called with the following parameters:
        1785 times: no parameters
        2080 times: prop=["sourceResource/stateLocatedIn"]
        """
        src = prop[0].split('/')[-1]  # remove sourceResource
        if src not in self.mapped_data:
            return self

        self.enrichment_report.append(
            f"[enrich_location]: not implemented, source field {src}")
        return self

    def enrich_type(self):
        """
        called with the following parameters:
        2081 times: no parameters
        """
        mapped_type = None

        record_types = self.mapped_data.get('type', [])
        if record_types:
            if not isinstance(record_types, list):
                record_types = [record_types]
            record_types = [
                t.get('#text')
                if isinstance(t, dict) else t
                for t in record_types
            ]
            record_types = [t.lower().rstrip('s') for t in record_types]

            for record_type in record_types:
                if record_type in constants.type_map:
                    mapped_type = constants.type_map[record_type]
                    break

        if not mapped_type:
            # try to get type from format
            record_formats = self.mapped_data.get('format', [])
            if not isinstance(record_formats, list):
                record_formats = [record_formats]
            record_formats = [f.lower().rstrip('s') for f in record_formats]
            for record_format in record_formats:
                if constants.format_map.get(record_format):
                    mapped_type = constants.format_map[record_format]
                    break

        self.mapped_data['type'] = mapped_type
        return self

    def enrich_subject(self):
        """
        called with the following parameters:
        2080 times: no parameters
        """
        # normalize subject
        subjects = self.mapped_data.get('subject', [])
        if isinstance(subjects, str):
            subjects = [subjects]
        if not subjects:
            subjects = []
        subjects = [
            subject.get('name') for subject in subjects
            if isinstance(subject, dict)
        ]

        def clean_subject(value):
            value = value.strip()
            regexps = (
                ('\s*-{2,4}\s*', '--'),
                ('\s*-\s*-\s*', '--'),
                ('^[\.\' ";]*', ''),
                ('[\.\' ";]*$', '')
            )
            for regexp, replacement in regexps:
                value = re.sub(regexp, replacement, value)
            value = value[:1].upper() + value[1:]
            return value

        # clean subject
        cleaned_subjects = [clean_subject(value) for value in subjects]
        # remove falsy values
        filtered_subjects = [{"name": s} for s in cleaned_subjects if s]
        # set subject
        self.mapped_data['subject'] = filtered_subjects
        return self

    def enrich_format(self):
        """
        called with the following parameters:
        2080 times: no parameters
        """
        format_regexps = [
            ('audio/mp3', 'audio/mpeg'), ('images/jpeg', 'image/jpeg'),
            ('image/jpg', 'image/jpeg'), ('image/jp$', 'image/jpeg'),
            ('img/jpg', 'image/jpeg'), ('^jpeg$', 'image/jpeg'),
            ('^jpg$', 'image/jpeg'), ('\W$', '')
        ]

        record_formats = self.mapped_data.get('format', [])
        if isinstance(record_formats, str):
            record_formats = [record_formats]

        mapped_format = []
        imt_values = []

        for record_format in record_formats:
            if record_format.startswith("http"):
                ext = os.path.splitext(record_format)[1].split('.')
                record_format = ext[1] if len(ext) == 2 else ""

            cleaned_format = record_format.lower().strip()
            for pattern, replace in format_regexps:
                cleaned_format = re.sub(pattern, replace, cleaned_format)
                cleaned_format = re.sub(
                    r"^([a-z0-9/]+)\s.*",
                    r"\1",
                    cleaned_format
                )

            imt_regexes = [re.compile('^' + x + '(/)') for x in constants.imt_types]
            if any(regex.match(cleaned_format) for regex in imt_regexes):
                # format is an IMT type as defined by dpla-ingestion
                if cleaned_format not in mapped_format:
                    mapped_format.append(cleaned_format)
                if cleaned_format not in imt_values:
                    imt_values.append(cleaned_format)
            else:
                if record_format not in mapped_format:
                    mapped_format.append(record_format)

        if mapped_format:
            self.mapped_data['format'] = mapped_format

        if imt_values:
            if not self.mapped_data.get('hasView/format'):
                self.mapped_data['hasView/format'] = imt_values
            if not self.mapped_data.get('type'):
                split_imt = [imt.split("/")[0] for imt in imt_values]
                format_2_type = {
                    "audio": "sound",
                    "image": "image",
                    "video": "moving image",
                    "text": "text"
                }
                type_map_imt = [format_2_type.get(imt) for imt in split_imt]
                self.mapped_data['type'] = type_map_imt

        return self

    def enrich_language(self):
        """
        called with the following parameters:
        2079 times: no parameters
        """
        languages = self.mapped_data.get('language', [])

        if not languages:
            return self

        if isinstance(languages, str):
            languages = [languages]

        iso_codes = []
        for language in filter(None, languages):
            if language in iso_639_3:
                iso_codes.append(language)
                continue

            # try to get an iso1 code from the language name
            stripped_language = re.sub("[\.\[\]\(\)]", "", language)
            cleaned_language = stripped_language.lower().strip()
            subbed_language = re.sub("[-_/].*$", "", cleaned_language)
            iso1 = subbed_language.strip()
            iso3 = iso_639_1.get(iso1, iso1)
            if iso3 in iso_639_3:
                iso_codes.append(iso3)
                continue

            # try to match a language regex
            match = None
            for iso3, regex in language_regexes.items():
                match = regex.match(language.strip())
                if match:
                    iso_codes.append(iso3)
                    break

            # try to match wb_language_regexes
            if not match:
                for iso3, regex in wb_language_regexes.items():
                    if regex.search(language):
                        iso_codes.append(iso3)

        if iso_codes:
            seen = set()
            # dedupe iso_codes
            iso_codes = [iso3 for iso3 in iso_codes
                         if not (iso3 in seen and seen.add(iso3))]
            mapped_language = [
                {"iso639_3": iso3, "name": iso_639_3[iso3]}
                for iso3 in iso_codes
            ]
            self.mapped_data['language'] = mapped_language
        return self

    def set_prop(self, prop, value):
        """
        set_prop is called with a prop, value, condition prop, condition
        value, and _dict flag (to indicate that value is a dict and should be
        loaded using json.loads()). We don't use condition prop, condition
        value, or _dict flag, so I've not implemented them here.

        called with the following parameters:
        2080 times: prop=sourceResource/stateLocatedIn
                    value=California
        """
        prop = prop[0].split('/')[-1]  # remove sourceResource
        self.mapped_data[prop] = value
        return self

    def unset_prop(self, prop):
        """
        unset_prop is called with a prop, condition, and condition_prop. We
        don't ever use condition or condition_prop so I've not implemented
        them here.

        called with the following parameters:
        1: prop=sourceResource/spatial
        2: prop=sourceResource/provenance
        """
        prop = prop[0].split('/')[-1]  # remove sourceResource
        if prop in self.mapped_data:
            del self.mapped_data[prop]
        return self

    def jsonfy_prop(self):
        """
        Some data is packed as strings that contain json. (UCSD)
        Take the data in the given property and turn any sub-values that can be
        read by json.loads into json object.

        called with the following parameters:
        293 times: no parameters
        """

        def jsonfy_obj(data):
            """
            Unpack JSON data from a list or dictionary. This method
            is recursive, and will iterate as deeply as it finds strings
            on a list or dictionary.
            """
            if isinstance(data, (int, float, bool)) or data is None:
                return data
            if isinstance(data, str):
                try:
                    x = json.loads(data)
                except (ValueError, TypeError):
                    x = data
                return x
            if isinstance(data, list):
                new_list = []
                for v in data:
                    try:
                        x = jsonfy_obj(v)
                        new_list.append(x)
                    except (ValueError, TypeError):
                        new_list.append(v)
                return new_list
            if isinstance(data, dict):
                obj_jsonfied = {}
                for key, value in list(data.items()):
                    obj_jsonfied[key] = jsonfy_obj(value)
                return obj_jsonfied

            return data

        self.source_metadata = jsonfy_obj(self.source_metadata)
        return self

    def drop_long_values(
            self, field: Optional[list[str]] = None, max_length=[150]):
        """ Look for long values in the sourceResource field specified.
        If value is longer than max_length, delete

        called with the following parameters:
        42 times: field=["description"], max_length=[150]
        8 times: field=["description"], max_length=[250]
        1 time: field=["description"], max_length=[1000]
        """
        if not field:
            return self

        field_name = field[0]
        max_length = max_length[0]

        fieldvalues = self.mapped_data.get(field_name, '')
        if isinstance(fieldvalues, list):
            new_list = []
            for item in fieldvalues:
                if item and len(item) <= int(max_length):
                    new_list.append(item)
            self.mapped_data[field_name] = new_list
        else:  # scalar
            if len(fieldvalues) > int(max_length):
                del self.mapped_data[field_name]

        return self

    def replace_regex(self, prop, regex, new=''):
        """
        Replaces a regex in prop

        called with the following parameters:
        10 times:   prop=sourceResource/publisher     regex=\$\S  new=--
        3 times:    prop=sourceResource/subject       regex=\$\S  new=--
        3 times:    prop=sourceResource/contributor   regex=\$\S  new=--
        1 time:     prop=sourceResource/creator       regex=\$\S  new=--
        """

        def recursive_regex_replace(value, regex_s, new):
            """Replace the regexs found in various types of data.
            Can be strings, lists or dictionaries
            This uses a regex to replace
            """
            if isinstance(value, str):
                regex = re.compile(regex_s)
                return regex.sub(new, value).strip()
            if isinstance(value, list):
                newlist = []
                for v in value:
                    newlist.append(recursive_regex_replace(v, regex_s, new))
                return newlist
            if isinstance(value, dict):
                for k, v in value.items():
                    value[k] = recursive_regex_replace(v, regex_s, new)
                return value
            return None

        prop = prop[0].split('/')[-1]  # remove sourceResource
        value = self.mapped_data[prop]
        regex = regex[0]
        new = new[0]
        new_value = recursive_regex_replace(value, regex, new)
        self.mapped_data[prop] = new_value
        return self

    def replace_substring(self, prop, old, new=''):
        """
        Replaces a substring in prop

        called with the following parameters:
        2 times: prop=sourceResource/subject    old=[lcsh]      new=
        1 time:  prop=sourceResource/subject    old=[lcna]      new=
        1 time:  prop=sourceResource/subject    old=[aacr2]     new=
        3 times: prop=sourceResource/title      old=[graphic]   new=
        3 times: prop=sourceResource/title      old=[graphic    new=
        3 times: prop=sourceResource/title      old=graphic]    new=
        3 times: prop=sourceResource/title      old=[graphic[   new=
        """

        def recursive_substring_replace(value, old, new):
            '''Replace the substrings found in various types of data.
            Can be strings, lists or dictionaries
            '''
            if isinstance(value, str):
                return value.replace(old, new).strip()
            if isinstance(value, list):
                newlist = []
                for v in value:
                    newlist.append(recursive_substring_replace(v, old, new))
                return newlist
            if isinstance(value, dict):
                for k, v in list(value.items()):
                    value[k] = recursive_substring_replace(v, old, new)
                return value
            return None

        prop = prop[0].split('/')[-1]  # remove sourceResource
        value = self.mapped_data[prop]
        self.mapped_data[prop] = recursive_substring_replace(value, old[0], new)
        return self

    def filter_fields(self, **_):
        """
        called with the following parameters:
        2333 times: keys=["sourceResource"]

        TODO: this recursed in the dpla-ingestion codebase.
        afaik, we're not implementing a deeply nested data structure
        so I'm not implementing recursion here.
        """
        del_keys = []
        for key, val in self.mapped_data.items():
            if not val:
                del_keys.append(key)

        for key in del_keys:
            del self.mapped_data[key]

        return self

    def set_ucldc_dataprovider(self, collection):
        """
        2333 times: no parameters
        """
        repo = collection['repository'][0]
        campus = None
        if repo.get('campus'):
            campus = repo['campus'][0]
        data_provider = repo['name']
        if campus:
            data_provider = f"{campus['name']}, {repo['name']}"
        # TODO: deprecate dataProvider, use 'repository' instead
        self.mapped_data['dataProvider'] = data_provider
        self.mapped_data['repository'] = data_provider
        # TODO: deprecate provider, use 'collection' instead
        self.mapped_data['provider'] = {
            'name': data_provider,
            '@id': collection['id']
        }
        self.mapped_data['collection'] = [{
            'id': collection['id'],
            'name': collection['name'],
            'repository': collection['repository'],
            'harvest_type': collection['harvest_type']
        }]
        self.mapped_data['stateLocatedIn'] = [{'name': 'California'}]
        return self

    def dedupe_sourceresource(self):
        """
        Remove blank values and duplicate values from self.mapped_data

        2333 times: no parameters
        """
        for key, value in self.mapped_data.copy().items():
            if not value:
                del self.mapped_data[key]
            if value == [u'none'] or value == [u'[none]']:
                del self.mapped_data[key]

        for key, value in self.mapped_data.items():
            if isinstance(value, list):
                # can't use set() because of dict values (non-hashable)
                new_list = []
                for item in value:
                    if item not in new_list:
                        new_list.append(item)
                self.mapped_data[key] = new_list
        return self

    def strip_html(self):
        """Strip HTML tags and whitespace from strings within the given object

        Remove HTML tags and convert HTML entities to UTF-8 characters.
        Compacts consecutive whitespace to single space characters, and strips
        whitespace from ends of strings.

        1660 times: no parameters
        """

        def _strip_html(obj):
            if isinstance(obj, str):
                return Markup(obj).striptags().strip()
            elif isinstance(obj, list):
                return [_strip_html(v) for v in obj]
            elif isinstance(obj, dict):
                return {k: _strip_html(v) for k, v in obj.items()}
            else:
                return obj

        self.mapped_data = _strip_html(self.mapped_data)
        return self

    def set_context(self):
        """
        We don't have an ingestType so this is a no-op. I don't think it
        matters much though - these URLs are 404s anyway. Keeping it here for
        posterity, and because it is a part of our existing enrichment chains.
        Once we have migrated to using Rikolti, it would be good to revisit
        simplifying the enrichment chain.

        994 times: no parameters
        """
        item_context = {
            "@context": "http://dp.la/api/items/context",
            "aggregatedCHO": "#sourceResource",
            "@type": "ore:Aggregation"
        }

        collection_context = {
            "@context": "http://dp.la/api/collections/context",
            "@type": "dcmitype:Collection"
        }

        if self.mapped_data.get("ingestType") == "item":
            self.mapped_data.update(item_context)
        elif self.mapped_data.get("ingestType"):
            self.mapped_data.update(collection_context)

        return self

    def capitalize_value(self, exclude):
        """
        the dpla-ingestion codebase takes parameters `prop` and `exclude`
        we never actually use the `prop` paramter, so I haven't implemented
        it here, instead just supplying the default list as part of the
        function definition.

        called with parameters:
        994 times:  exclude=["sourceResource/relation"]

        TODO: since this is always called with the same exclude pattern, we
        could just modify this to always exclude relation.
        """
        props = [
            "sourceResource/language",
            "sourceResource/title",
            "sourceResource/rights",
            "sourceResource/creator",
            "sourceResource/relation",
            "sourceResource/publisher",
            "sourceResource/subject",
            "sourceResource/description",
            "sourceResource/collection/title",
            "sourceResource/contributor",
            "sourceResource/extent",
            "sourceResource/format",
            # "sourceResource/spatial/currentLocation",  # State Located In
            # "sourceResource/spatial",  # place name?
            "dataProvider",
            "provider/name"
        ]

        def capitalize_str(value):
            return f"{value[0].upper()}{value[1:]}" if len(value) > 0 else ""

        for field in props:
            if field in exclude:
                continue
            if field.startswith('sourceResource'):
                field = field.split('/')[-1]
            if field in self.mapped_data:
                if isinstance(self.mapped_data[field], str):
                    val = self.mapped_data[field]
                    self.mapped_data[field] = capitalize_str(val)
                elif isinstance(self.mapped_data[field], list):
                    self.mapped_data[field] = [
                        # This doesn't map dictionary values; it leaves them untouched
                        capitalize_str(v) if isinstance(v, str) else v
                        for v in self.mapped_data[field]
                    ]

        return self

    def cleanup_value(self):
        """
        2076 times: no parameters
        """
        # Previously in default fields, not sure how they map to new system:
        # "sourceResource/collection/title",
        # "sourceResource/collection/description",
        # "sourceResource/contributor", "sourceResource/spatial/name"
        default_fields = [
            "language", "title", "creator", "relation",
            "publisher", "subject", "date",
        ]
        # Previously in dont strip trailing dot fields, 
        # not sure how they map to new system:
        # "hasView/format", "sourceResource/collection/title"
        dont_strip_trailing_dot = ["format", "extent", "rights", "place"]

        def cleanup(value, field):
            strip_dquote = '"' if field not in ["title", "description"] else ''
            strip_dot = '.' if field not in dont_strip_trailing_dot else ''
            strip_leading = '[\.\' \r\t\n;,%s]*' % (strip_dquote)
            strip_trailing = '[%s\' \r\t\n;,%s]*' % (strip_dot, strip_dquote)

            regexps = ('\( ', '('), \
                      (' \)', ')'), \
                      (' *-- *', '--'), \
                      ('[\t ]{2,}', ' '), \
                      ('^' + strip_leading, ''), \
                      (strip_trailing + '$', '')

            if isinstance(value, str):
                value = value.strip()
                for pattern, replacement in regexps:
                    value = re.sub(pattern, replacement, value)
            return value

        for field in default_fields + dont_strip_trailing_dot:
            if field not in self.mapped_data:
                continue
            if isinstance(self.mapped_data[field], str):
                self.mapped_data[field] = cleanup(
                    self.mapped_data[field], field)
            elif isinstance(self.mapped_data[field], list):
                self.mapped_data[field] = [
                    cleanup(v, field) for v in self.mapped_data[field]
                ]

        return self

    def geocode(self):
        """
        Geocode is only applied to two collections:
        https://calisphere.org/collections/6711/ (2 items)
        https://calisphere.org/collections/22322/ (1 item)

        the geocode enrichment is long and complex - if it turns out we need
        the geocode enrichment, we can add it back in.
        """
        return self

    def unescape_xhtml_entities(self):
        """
        unescape xhtml entities is only applied to one collection:
        https://calisphere.org/collections/27106
        with parameter ?field=sourceResource

        leaving implementation of this for later
        """

        return self

    # TODO: this should get moved into validation, if it continues to
    # exist at all - ported here from existing codebase
    # def has_required_fields(self):
    #     record_id = f"---- OMITTED: Doc:{self.legacy_couch_db_id}"
    #     error = False

    #     for field in ['title', 'rights', 'rightsURI', 'isShownAt', 'type']:
    #         if field not in self.mapped_data:
    #             print(f"{record_id} has no {field}.")
    #             error = True

    #     # check that value in isShownAt is at least a valid URL format
    #     is_shown_at = self.mapped_data['isShownAt']
    #     parsed = urlparse(is_shown_at)
    #     if not any([
    #         parsed.scheme, parsed.netloc, parsed.path,
    #         parsed.params, parsed.query
    #     ]):
    #         print(
    #             f"{record_id} isShownAt is not a URL: {is_shown_at}"
    #         )
    #         error = True

    #     # if record is image but doesnt have a reference_image_md5, reject
    #     if (not isinstance(self.mapped_data['type'], list) and
    #         self.mapped_data['type'].lower() == 'image'):
    #         if 'object' not in self.mapped_data:
    #             print(f"{record_id} has no harvested image.")
    #             error = True

    #     if error:
    #         return False
    #     return True

    def remove_none_values(self):
        keys_to_remove = []
        for field in self.mapped_data.keys():
            if self.mapped_data[field] is None:
                keys_to_remove.append(field)
            elif isinstance(self.mapped_data[field], list):
                if None in self.mapped_data[field]:
                    self.mapped_data[field].remove(None)
                if len(self.mapped_data[field]) == 0:
                    keys_to_remove.append(field)
        for key in keys_to_remove:
            del self.mapped_data[key]
        return self

    # TODO: analyze this against enrichment chain to determine
    # how much is necessary/how much is redundant
    def solr_updater(self):

        def normalize_sort_field(sort_field,
                                 default_missing='~title unknown',
                                 missing_equivalents=['title unknown']):
            if sort_field:
                sort_field = sort_field.lower()
                # remove punctuation
                re_alphanumspace = re.compile(r'[^0-9A-Za-z\s]*')
                sort_field = re_alphanumspace.sub('', sort_field)
                words = sort_field.split()
                if words:
                    if words[0] in ('the', 'a', 'an'):
                        sort_field = ' '.join(words[1:])
            if not sort_field or sort_field in missing_equivalents:
                sort_field = default_missing
            return sort_field

        def dejson(data):
            '''de-jsonfy the data.
            For valid json strings, unpack in sensible way?
            '''
            if not data:
                return data

            if isinstance(data, list):
                dejson_data = [dejson(d) for d in data]
            elif isinstance(data, dict):
                # If there's only one item in the dictionary, we assume the
                # value we want is the only value in the dictionary. This was done
                # because collection 184 had data that looks like this:
                # `{'genre': ['Oral histories--California--San Diego--1980-1989']}`
                dejson_data = data.get(
                    'item', data.get(
                        'name', data.get(
                            'text', None)))
                if not dejson_data:
                    items = data.items()
                    if len(items) == 1:
                        dejson_data = list(items)[0][1]
                if not dejson_data:
                    return data

            else:
                try:
                    j = json.loads(str(data))
                    flatdata = j.get('name', data)
                except (ValueError, AttributeError):
                    flatdata = data
                    pass
                dejson_data = flatdata
            return dejson_data

        def filter_blank_values(data):
            '''For a given field_src  in the data, create a dictionary to
            update the field_dest with.
            If no values, make the dict {}, this will avoid empty data values
            '''
            if not data:
                return []

            items = dejson(data)

            if isinstance(items, str) and items:
                items_not_blank = items
            else:
                items_not_blank = [i for i in items if i]
            return items_not_blank

        def unpack_display_date(date_obj):
            '''Unpack a couchdb date object'''
            if not isinstance(date_obj, list):
                date_obj = [date_obj]

            dates = []
            for dt in date_obj:
                if isinstance(dt, dict):
                    displayDate = dt.get('displayDate', None)
                elif isinstance(dt, str):
                    displayDate = dt
                else:
                    displayDate = None
                dates.append(displayDate)
            return dates

        def get_facet_decades(date_value):
            '''Return set of decade string for given date structure.
            date is a dict with a "displayDate" key.
            '''
            if isinstance(date_value, dict):
                facet_decades = facet_decade(
                    date_value.get('displayDate', ''))
            else:
                facet_decades = facet_decade(str(date_value))
            facet_decade_set = set()  # don't repeat values
            for decade in facet_decades:
                facet_decade_set.add(decade)
            return

        def facet_decade(date_string):
            """ process string and return array of decades """
            year = date.today().year
            pattern = re.compile(r'(?<!\d)(\d{4})(?!\d)')
            matches = [int(match) for match in re.findall(pattern, date_string)]
            matches = list(filter(lambda a: a >= 1000, matches))
            matches = list(filter(lambda a: a <= year, matches))
            if not matches:
                return ['unknown']
            start = (min(matches) // 10) * 10
            end = max(matches) + 1
            return map('{0}s'.format, range(start, end, 10))

        def add_facet_decade(record):
            '''Add the facet_decade field to the solr_doc dictionary
            If no date field in sourceResource, pass fake value to set
            as 'unknown' in solr_doc
            '''
            if 'date' in record:
                dates = record['date']
                if not isinstance(dates, list):
                    dates = [dates]
                for date_value in dates:
                    try:
                        facet_decades = get_facet_decades(date_value)
                        if facet_decades and len(facet_decades) == 1:
                            facet_decades = facet_decades[0]
                        return facet_decades
                    except AttributeError as e:
                        print(
                            'Attr Error for facet_decades in doc:{} ERROR:{}'.
                            format(record['_id'], e))
            else:
                facet_decades = get_facet_decades('none')
                return facet_decades

        def find_ark_in_identifiers(identifiers):
            re_ark_finder = re.compile(r'(ark:/\d\d\d\d\d/[^/|\s]*)')
            for identifier in identifiers:
                if identifier:
                    match = re_ark_finder.search(identifier)
                    if match:
                        return match.group(0)
            return None

        def ucla_ark(doc):
            '''UCLA ARKs are buried in a mods field in originalRecord:
            "mods_recordInfo_recordIdentifier_mlt": "21198-zz002b1833",
            "mods_recordInfo_recordIdentifier_s": "21198-zz002b1833",
            "mods_recordInfo_recordIdentifier_t": "21198-zz002b1833",
            If one is found, safe to assume UCLA & make the ARK
            NOTE: I cut & pasted this to the ucla_solr_dc_mapper to get it
            into the "identifier" field
            '''
            ark = None
            id_fields = ("mods_recordInfo_recordIdentifier_mlt",
                         "mods_recordInfo_recordIdentifier_s",
                         "mods_recordInfo_recordIdentifier_t")
            for f in id_fields:
                try:
                    mangled_ark = doc['originalRecord'][f]
                    naan, arkid = mangled_ark.split('-')  # could fail?
                    ark = '/'.join(('ark:', naan, arkid))
                    break
                except KeyError:
                    pass
            return ark

        # TODO: I wonder if rather than taking a hash of the mapped _id value
        # to use for the solr ID, we should actually take a hash of the
        # original record (and maybe a hash of the mapped record?)
        def get_solr_id(couch_doc):
            ''' Extract a good ID to use in the solr index.
            see : https://github.com/ucldc/ucldc-docs/wiki/pretty_id
            arks are always pulled if found, gets first.
            Some institutions have known ark framents, arks are constructed
            for these.
            Nuxeo objects retain their UUID
            All other objects the couchdb _id is md5 sum
            '''
            # look in sourceResoure.identifier for an ARK if found return it
            solr_id = find_ark_in_identifiers(
                couch_doc.get('identifier', []))
            if not solr_id:
                # no ARK in identifiers. See if is a nuxeo object
                collection = couch_doc['collection'][0]
                harvest_type = collection['harvest_type']
                if harvest_type == 'nuxeo':
                    solr_id = couch_doc.get('calisphere-id', None)
                else:
                    solr_id = None
            if not solr_id:
                solr_id = ucla_ark(couch_doc)
            if not solr_id:
                if not couch_doc.get('calisphere-id'):
                    raise Exception('no calisphere id')
                hash_id = hashlib.md5()
                hash_id.update((
                    f"{couch_doc['collection'][0]['id']}--"
                    f"{couch_doc['calisphere-id']}"
                ).encode('utf-8'))
                solr_id = hash_id.hexdigest()
            return solr_id

        def map_couch_to_solr_doc(record):
            collections = record['collection']

            def sort_col_data(collection):
                '''Return the string form of the collection data.
                sort_collection_data ->
                [sort_collection_name::collection_name::collection_url, <>,<>]
                '''
                sort_name = normalize_sort_field(
                    collection['name'],
                    default_missing='~collection unknown',
                    missing_equivalents=[])
                sort_string = ':'.join((sort_name, collection['name'],
                                        str(collection['id'])))
                return sort_string

            if not all([c.get('repository') for c in collections]):
                raise Exception
            repos = [
                repo for c in collections for repo in c.get('repository', [])
            ]

            def compose_repo_data(repo):
                repo_data = f"{repo['id']}::{repo['name']}"
                if 'campus' in repo and len(repo['campus']):
                    repo_data = f"{repo_data}::{repo['campus'][0]['name']}"
                return repo_data

            solr_doc = {
                'calisphere-id': record.get('calisphere-id'),
                'is_shown_at': record.get('isShownAt'),
                'is_shown_by': record.get('isShownBy'),
                'harvest_id_s': record.get('_id'),
                'reference_image_md5': record.get('object'),
                'url_item': record.get('isShownAt'),
                'item_count': record.get('item_count', 0),

                # registry fields
                'collection_url': [c['id'] for c in collections],
                'collection_name': [c['name'] for c in collections],
                'collection_data': [f"{c['id']}::{c['name']}"
                                    for c in collections],
                'sort_collection_data': [sort_col_data(c) for c in collections],

                'repository_url': [repo['id'] for repo in repos],
                'repository_name': [repo['name'] for repo in repos],
                'repository_data': [compose_repo_data(repo) for repo in repos],

                # source resource fields
                'alternative_title': filter_blank_values(
                    record.get('alternativeTitle')),
                'contributor': filter_blank_values(record.get('contributor')),
                # TODO: coverage is listed twice here - why?
                # commenting out the first
                # 'coverage': filter_blank_values(record.get('coverage')),
                'creator': filter_blank_values(record.get('creator')),
                'description': filter_blank_values(record.get('description')),
                'extent': filter_blank_values(record.get('extent')),
                'format': filter_blank_values(record.get('format')),
                'genre': filter_blank_values(record.get('genre')),
                'identifier': filter_blank_values(record.get('identifier')),
                'publisher': filter_blank_values(record.get('publisher')),
                'relation': filter_blank_values(record.get('relation')),
                'rights': filter_blank_values(record.get('rights')),
                'rights_uri': filter_blank_values(record.get('rightsURI')),
                'title': filter_blank_values(record.get('title')),
                'type': filter_blank_values(record.get('type')),
                'provenance': filter_blank_values(record.get('provenance')),
                'spatial': filter_blank_values(record.get('spatial')),
                'coverage': filter_blank_values(record.get('spatial')),
                'date': unpack_display_date(record.get('date')),
                'language': [
                    lang.get('name', lang.get('iso639_3'))
                    if isinstance(lang, dict) else lang
                    for lang in record.get('language', [])
                ],
                'subject': [
                    s['name'] if isinstance(s, dict) else dejson(s)
                    for s in record.get('subject', [])
                ],
                'temporal': unpack_display_date(record.get('temporal')),

                # original record fields
                'rights_date': filter_blank_values(record.get('dateCopyrighted')),
                'rights_holder': filter_blank_values(record.get('rightsHolder')),
                'rights_note': filter_blank_values(record.get('rightsNote')),
                'source': filter_blank_values(record.get('source')),
                'structmap_text': filter_blank_values(
                    record.get('structmap_text')),
                'structmap_url': filter_blank_values(
                    record.get('structmap_url')),
                'transcription': filter_blank_values(
                    record.get('transcription')),
                'location': filter_blank_values(
                    record.get('location'))
            }

            solr_doc['media_source'] = record.get('media_source', {})
            solr_doc['thumbnail_source'] = record.get('isShownBy', {})

            campuses = [
                campus for c in collections for campus in c.get('campus', [])
            ]
            if campuses:
                solr_doc['campus_url'] = [c['id'] for c in campuses]
                solr_doc['campus_name'] = [c['name'] for c in campuses]
                solr_doc['campus_data'] = [f"{c['id']}::{c['name']}"
                                           for c in campuses]

            if record.get('object_dimensions'):
                solr_doc['reference_image_dimensions'] = (
                    f"{record.get('object_dimensions', [])[0]}:"
                    f"{record.get('object_dimensions', [])[1]}"
                )

            if record.get('date'):
                date_source = record.get('date', None)
                if not isinstance(date_source, list):
                    date_source = [date_source]
                dates_start = [make_datetime(dt.get("begin"))
                               for dt in date_source
                               if isinstance(dt, dict) and dt.get("begin")]
                dates_start = sorted(filter(None, dates_start))

                start_date = \
                    dates_start[0].strftime("%Y-%m-%d") if dates_start else None

                dates_end = [make_datetime(dt.get("end"))
                             for dt in date_source
                             if isinstance(dt, dict) and dt.get("end")]
                dates_end = sorted(filter(None, dates_end))

                # TODO: should this actually be the last date?
                end_date = dates_end[0].strftime("%Y-%m-%d") if dates_end else None

                # fill in start_date == end_date if only one exists
                start_date = end_date if not start_date else start_date
                end_date = start_date if not end_date else end_date

                solr_doc['sort_date_start'] = start_date
                solr_doc['sort_date_end'] = end_date

            # normalize type
            solr_types = solr_doc.get('type', [])
            normalized_types = []
            if not isinstance(solr_types, list):
                solr_types = [solr_types]
            for solr_type in solr_types:
                lowercase_dcmi = [v.lower() for v in constants.dcmi_types.values()]
                if solr_type not in lowercase_dcmi:
                    if 'physical' in solr_type.lower():
                        solr_type = 'physical object'
                    elif 'moving' in solr_type.lower():
                        solr_type = 'moving image'
                normalized_types.append(solr_type)
            solr_doc['type'] = normalized_types

            # add sort title
            if isinstance(record['title'], str):
                sort_title = record['title']
            else:
                sort_title = record['title'][0]
            if 'sort-title' in record:  # OAC mostly
                sort_obj = record['sort-title']
                if isinstance(sort_obj, list):
                    sort_obj = sort_obj[0]
                    if isinstance(sort_obj, dict):
                        sort_title = sort_obj.get(
                            'text', record['title'][0])
                    else:
                        sort_title = sort_obj
                else:  # assume flat string
                    sort_title = sort_obj
            solr_doc['sort_title'] = normalize_sort_field(sort_title)

            solr_doc['facet_decade'] = add_facet_decade(record)
            solr_doc['id'] = get_solr_id(record)

            keys = list(solr_doc.keys())
            keys.sort()

            return {i: solr_doc[i] for i in keys}

        def make_datetime(date_string):
            date_time = None

            #  This matches YYYY or YYYY-MM-DD
            match = re.match(
                r"^(?P<year>[0-9]{4})"
                r"(-(?P<month>[0-9]{1,2})"
                r"-(?P<day>[0-9]{1,2}))?$", date_string)
            if match:
                year = int(match.group("year"))
                month = int(match.group("month") or 1)
                day = int(match.group("day") or 1)
                date_time = datetime(year, month, day, tzinfo=timezone.utc)

                try:
                    date_time = datetime(year, month, day, tzinfo=timezone.utc)
                except Exception as e:
                    print(f"Error making datetime: {e}")
                    pass

            return date_time

        def check_nuxeo_media(record):
            '''Check that the media_json and jp2000 exist for a given solr doc.
            Raise exception if not
            '''
            if 'structmap_url' not in record:
                return
            # check that there is an object at the structmap_url
            # try:
            #     MediaJson(doc['structmap_url']).check_media()
            # except ClientError as e:
            #     message = '---- OMITTED: Doc:{} missing media json {}'.format(
            #         doc['harvest_id_s'],
            #         e)
            #     print(message, file=sys.stderr)
            #     raise MissingMediaJSON(message)
            # except ValueError as e:
            #     message = (
            #         f"---- OMITTED: Doc:{doc['harvest_id_s']} "
            #         f"Missing reference media file: {e}"
            #     )
            #     print(message, file=sys.stderr)
            #     raise MediaJSONError(message)

        # set a default title if none exists
        if not self.mapped_data.get('title'):
            self.mapped_data['title'] = ['Title unknown']

        # self.has_required_fields()
        self.mapped_data = map_couch_to_solr_doc(self.mapped_data)
        check_nuxeo_media(self.mapped_data)
        return self
