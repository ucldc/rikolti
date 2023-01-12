import os
import json
import re
import boto3

from abc import ABC, abstractmethod
from markupsafe import Markup
from typing import Any, Union

import settings

from . import constants
from .iso639_1 import iso_639_1
from .iso639_3 import iso_639_3, language_regexes, wb_language_regexes
from typing import Callable


class UCLDCWriter(object):
    def __init__(self, payload):
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def write_local_mapped_metadata(self, mapped_metadata):
        local_path = settings.local_path(
            'mapped_metadata', self.collection_id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "w+")
        page.write(json.dumps(mapped_metadata))

    def write_s3_mapped_metadata(self, mapped_metadata):
        s3_client = boto3.client('s3')
        bucket = 'rikolti'
        key = f"mapped_metadata/{self.collection_id}/{self.page_filename}"
        s3_client.put_object(
            ACL='bucket-owner-full-control',
            Bucket=bucket,
            Key=key,
            Body=json.dumps(mapped_metadata))


class Vernacular(ABC, object):
    def __init__(self, payload: dict) -> None:
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def get_api_response(self) -> dict:
        if settings.DATA_SRC == 'local':
            return self.get_local_api_response()
        else:
            return self.get_s3_api_response()

    def get_local_api_response(self) -> str:
        local_path = settings.local_path(
            'vernacular_metadata', self.collection_id)
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "r")
        api_response = page.read()
        return api_response

    def get_s3_api_response(self) -> str:
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"vernacular_metadata/{self.collection_id}/{self.page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        api_response = s3_obj_summary['Body'].read()
        return api_response


class Record(ABC, object):

    def __init__(self, collection_id: int, record: dict[str, Any]):
        self.collection_id: int = collection_id
        self.source_metadata: dict = record

    def to_dict(self) -> dict[str, Any]:
        return self.mapped_metadata

    def to_UCLDC(self) -> dict[str, Any]:
        """
        Maps source metadata to UCLDC format, saving result to
        self.mapped_metadata.

        Returns: dict
        """
        self.mapped_metadata = {}

        supermaps = [
            super(c, self).UCLDC_map()
            for c in list(reversed(type(self).__mro__))
            if hasattr(super(c, self), "UCLDC_map")
        ]
        for map in supermaps:
            self.mapped_metadata = {**self.mapped_metadata, **map}
        self.mapped_metadata = {**self.mapped_metadata, **self.UCLDC_map()}

        return self.mapped_metadata

    def UCLDC_map(self) -> dict:
        """
        Defines mappings from source metdata to UCDLC that are specific
        to this implementation.

        All dicts returned by this method up the ancestor chain
        are merged together to produce a final result.
        """
        return {
            "isShownAt": self.map_is_shown_at(),
            "isShownBy": self.map_is_shown_by()
        }

    @abstractmethod
    def map_is_shown_at(self) -> Union[str, None]:
        pass

    @abstractmethod
    def map_is_shown_by(self) -> Union[str, None]:
        pass

    # Mapper Helpers
    def collate_subfield(self, field: str, subfield: str) -> list:
        return [f[subfield] for f in self.source_metadata.get(field, [])]

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
        """
        id_values = self.mapped_data.get("identifier")
        if not isinstance(id_values, list):
            id_values = [id_values]

        ark_ids = [v.get('text') for v in id_values
                   if v.get('text').startswith("http://ark.cdlib.org/ark:")]

        calisphere_id = None
        if ark_ids:
            calisphere_id = ark_ids[0]
        else:
            calisphere_id = id_values[0]

        self.mapped_data["id"] = f"{self.collection_id}--{calisphere_id}"
        self.mapped_data["isShownAt"] = calisphere_id
        self.mapped_data["isShownBy"] = f"{calisphere_id}/thumbnail"
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

        self.mapped_data['id'] = f"{self.collection_id}--{calisphere_id}"
        return self

    def select_preservica_id(self):
        calisphere_id = self.mapped_data.get("preservica_id", {}).get('$')
        self.mapped_data['id'] = f"{self.collection_id}--{calisphere_id}"
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
        mode = mode[0]

        if field == "rights":
            rights = [
                constants.rights_status.get(collection.get('rights_status')),
                collection.get('rights_statement')
            ]
            rights = [r for r in rights if r]
            if not rights:
                rights = [constants.rights_statement_default]
            field_value = rights

        if field == "type":
            field_value = [constants.dcmi_types.get(collection.get('dcmi_type'), None)]

        if field == "title":
            field_value = ["Title unknown"]

        if mode == "overwrite":
            self.mapped_data[field] = field_value
        elif mode == "append":
            if field in self.mapped_data:
                self.mapped_data[field] += (field_value)
            else:
                self.mapped_data[field] = field_value
        else:   # default is fill if empty
            if field not in self.mapped_data:
                self.mapped_data[field] = field_value

        # not sure what this is about
        # if not exists(data, "@context"):
            # self.mapped_data["@context"] = "http://dp.la/api/items/context"
        return self

    def shred(self, field, delim=";"):
        """
        Based on DPLA-Ingestion service that accepts a JSON document and
        "shreds" the value of the field named by the "prop" parameter

        Splits values by delimeter; handles some complex edge cases beyond what
        split() expects. For example:
            ["a,b,c", "d,e,f"] -> ["a","b","c","d","e","f"]
            'a,b(,c)' -> ['a', 'b(,c)']
        Duplicate values are removed.

        called with the following parameters:
        2,078 times:    field=["sourceResource/spatial"],       delim = ["--"]
        1,021 times:    field=["sourceResource/subject/name"]
        1,021 times:    field=["sourceResource/creator"]
        1,021 times:    field=["sourceResource/type"]
        5 times:        field=["sourceResource/description"],   delim=["<br>"]
        """
        field = field[0].split('/')[1:]     # remove sourceResource
        delim = delim[0]

        if field not in self.mapped_data:
            return self

        value = self.mapped_data[field]
        if isinstance(value, list):
            try:
                value = delim.join(value)
                value = value.replace(f"{delim}{delim}", delim)
            except Exception as e:
                print(
                    f"Can't join list {value} on delim for "
                    f"{self.mapped_data['id']}, {e}"
                )
        if delim not in value:
            return self

        shredded = value.split(re.escape(delim))
        shredded = [s.strip() for s in shredded if s.strip()]
        result = []
        for s in shredded:
            if s not in result:
                result.append(s)
        self.mapped_data[field] = result

        return self

    def copy_prop(self, prop, to_prop, skip_if_exists=[False]):
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

        src = prop[0].split('/')[1:]
        dest = to_prop[0].split('/')[1:]
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
            print(
                f"Prop {src} is {type(src_val)} and prop {dest} is "
                f"{type(dest_val)} - not a string/list for record "
                f"{self.mapped_data.get('id')}"
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
        src = prop[0].split('/')[-1]   # remove sourceResource
        if src not in self.mapped_data:
            return self

        src_values = self.mapped_data[src]
        if isinstance(src_values, str):
            src_values = [src_values]
        remove = []
        dest_values = self.mapped_data.get(dest, [])
        if isinstance(dest_values, str):
            dest_values = [dest_values]

        for value in src_values:
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

    def lookup(self, prop, target, substitution, inverse=False):
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
        src = prop[0].split('/')[-1]  # remove sourceResource
        dest = target[0].split('/')[-1]  # remove sourceResource
        substitution = substitution[0]
        inverse = bool(inverse[0] in ['True', 'true'])

        # TODO: this won't actually work for deeply nested fields

        if src not in self.mapped_data:
            print(
                f"Source field {src} not in "
                "record {self.mapped_data.get('id')}"
            )
            return self

        src_values = self.mapped_data[src]
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
        if src not in self.mapped_data():
            return self

        print('enrich_location not implemented')
        return self

    def enrich_type(self):
        """
        called with the following parameters:
        2081 times: no parameters
        """
        record_types = self.mapped_data.get('type', [])
        if not isinstance(record_types, list):
            record_types = [record_types]
        record_types = [
            t.get('#text', t.get('text'))
            if isinstance(t, dict) else t
            for t in record_types
        ]
        record_types = [t.lower().rstrip('s') for t in record_types]
        mapped_type = None
        for record_type in record_types:
            if constants.type_mape[record_type]:
                mapped_type = constants.type_map[record_type]
                break

        if not mapped_type:
            # try to get type from format
            record_formats = self.mapped_data.get('format', [])
            if not isinstance(record_formats, list):
                record_formats = [record_formats]
            record_formats = [f.lower().rstrip('s') for f in record_formats]
            for record_format in record_formats:
                if constants.format_map[record_format]:
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
        if isinstance(languages, str):
            languages = [languages]

        iso_codes = []
        for language in languages:
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
            for regex, iso3 in language_regexes.items():
                match = regex.match(language.strip())
                if match:
                    iso_codes.append(iso3)
                    break

            # try to match wb_language_regexes
            if not match:
                for regex, iso3 in wb_language_regexes.items():
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
        prop = prop.split('/')[-1]  # remove sourceResource
        self.mapped_data[prop] = value
        return self

    def unset_prop(self, prop, value):
        """
        unset_prop is called with a prop, condition, and condition_prop. We
        don't ever use condition or condition_prop so I've not implemented
        them here.

        called with the following parameters:
        1: prop=sourceResource/spatial
        2: prop=sourceResource/provenance
        """
        prop = prop.split('/')[-1]  # remove sourceResource
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
            '''Jsonfy a python dict object. For immediate sub items (not fully
            recursive yet) if the data can be turned into a json object, do so.
            Unpacks string json objects buried in some blacklight/solr feeds.
            '''
            obj_jsonfied = {}
            if isinstance(data, numbers.Number) or isinstance(data, bool):
                return data
            if isinstance(data, str):
                try:
                    x = json.loads(data)
                except (ValueError, TypeError) as e:
                    x = data
                return x
            for key, value in list(data.items()):
                if isinstance(value, list):
                    new_list = []
                    for v in value:
                        try:
                            x = jsonfy_obj(v)
                            new_list.append(x)
                        except (ValueError, TypeError) as e:
                            new_list.append(v)
                    obj_jsonfied[key] = new_list
                else:  # usually singlevalue string, not json
                    try:
                        x = json.loads(value)
                        # catch numbers already typed as singlevalue strings
                        if isinstance(x, int):
                            x = value
                    except (ValueError, TypeError) as e:
                        x = value
                    obj_jsonfied[key] = x
            return obj_jsonfied

        obj_jsonfied = jsonfy_obj(self.mapped_data)
        return json.dumps(obj_jsonfied)

    def drop_long_values(self, field=None, max_length=150):
        """ Look for long values in the sourceResource field specified.
        If value is longer than max_length, delete

        called with the following parameters:
        42 times: field=description, max_length=150
        8 times: field=description, max_length=250
        1 time: field=description, max_length=1000
        """
        fieldvalues = self.mapped_data.get(field)
        if isinstance(fieldvalues, list):
            new_list = []
            for item in fieldvalues:
                if len(item) <= int(max_length):
                    new_list.append(item)
            self.mapped_data[field] = new_list
        else:  # scalar
            if len(fieldvalues) > int(max_length):
                del self.mapped_data[field]

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

        prop = prop.split('/')[-1]  # remove sourceResource
        value = self.mapped_data[prop]
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

        prop = prop.split('/')[-1]  # remove sourceResource
        value = self.mapped_data[prop]
        new_value = recursive_substring_replace(value, old, new)
        self.mapped_data[prop] = new_value
        return self

    def filter_fields(self, keys):
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
        if len(repo['campus']):
            campus = repo['campus'][0]
        data_provider = repo['name']
        if campus:
            data_provider = f"{campus['name']}, {repo['name']}"
        self.mapped_data['dataProvider'] = data_provider
        self.mapped_data['provider'] = {
            'name': data_provider,
            '@id': collection['id']
        }
        self.mapped_data['stateLocatedIn'] = [{'name': 'California'}]
        return self

    def dedupe_sourceresource(self):
        """
        Remove blank values and duplicate values from self.mapped_data

        2333 times: no parameters
        """
        for key, value in self.mapped_data.items():
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
        for field in props:
            if field in exclude:
                continue
            if field.startswith('sourceResource'):
                field = field.split('/')[-1]
            if field in self.mapped_data:
                if isinstance(self.mapped_data[field], str):
                    val = self.mapped_data[field]
                    self.mapped_data[field] = f"{val[0].upper()}{val[1:]}"
                elif isinstance(self.mapped_data[field], list):
                    self.mapped_data[field] = [
                        f"{v[0].upper()}{v[1:]}"
                        for v in self.mapped_data[field] if isinstance(v, str)
                    ]
        return self

    def cleanup_value(self):
        """
        2076 times: no parameters
        """
        default_fields = [
            "sourceResource/language", "sourceResource/title",
            "sourceResource/creator", "sourceResource/relation",
            "sourceResource/publisher", "sourceResource/subject",
            "sourceResource/date",
            "sourceResource/collection/title",
            "sourceResource/collection/description",
            "sourceResource/contributor", "sourceResource/spatial/name"
        ]
        dont_strip_trailing_dot = [
            "hasView/format", "sourceResource/format", "sourceResource/extent",
            "sourceResource/rights",
            "sourceResource/place", "sourceResource/collection/title"
        ]

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
            # TODO: this won't work for deeply nested fields
            field.split('/')[1]     # remove sourceResource

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
