import os
import json
import re
import boto3
from markupsafe import Markup   # used in Record.strip_html()

DEBUG = os.environ.get('DEBUG', False)


class VernacularReader(object):
    def __init__(self, payload):
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def local_path(self, folder):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir,
            'rikolti_bucket',
            folder,
            str(self.collection_id),
        ])
        return local_path

    def get_api_response(self):
        if DEBUG:
            return self.get_local_api_response()
        else:
            return self.get_s3_api_response()

    def get_local_api_response(self):
        local_path = self.local_path('vernacular_metadata')
        page_path = os.sep.join([local_path, str(self.page_filename)])
        page = open(page_path, "r")
        api_response = page.read()
        return api_response

    def get_s3_api_response(self):
        s3 = boto3.resource('s3')
        bucket = 'rikolti'
        key = f"vernacular_metadata/{self.collection_id}/{self.page_filename}"
        s3_obj_summary = s3.Object(bucket, key).get()
        api_response = s3_obj_summary['Body'].read()
        return api_response


class UCLDCWriter(object):
    def __init__(self, payload):
        self.collection_id = payload.get('collection_id')
        self.page_filename = payload.get('page_filename')

    def local_path(self, folder):
        parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
        local_path = os.sep.join([
            parent_dir,
            'rikolti_bucket',
            folder,
            str(self.collection_id),
        ])
        return local_path

    def write_local_mapped_metadata(self, mapped_metadata):
        local_path = self.local_path('mapped_metadata')
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


# repeating self here, how to get from avram?
RIGHTS_STATUS = {
    'CR': 'copyrighted',
    'PD': 'public domain',
    'UN': 'copyright unknown',
    # 'X':  'rights status unknown', # or should throw error?
}
RIGHTS_STATEMENT_DEFAULT = (
    "Please contact the contributing institution for more information "
    "regarding the copyright status of this object."
)
DCMI_TYPES = {
    'C': 'Collection',
    'D': 'Dataset',
    'E': 'Event',
    'I': 'Image',
    'F': 'Moving Image',
    'R': 'Interactive Resource',
    'V': 'Service',
    'S': 'Software',
    'A': 'Sound',   # A for audio
    'T': 'Text',
    'P': 'Physical Object',
    # 'X': 'type unknown' # default, not set
}
REGSEARCH = [
    (
        r"\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*"
        r"\d{1,4}\s*[-/]\s*\d{1,4}"
    ),
    r"\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}",
    r"\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}",
    r"\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}",
    r"\d{4}\s*[-/]\s*\d{4}",
    r"\d{1,2}\s*[-/]\s*\d{4}",
    r"\d{4}\s*[-/]\s*\d{1,2}",
    r"\d{4}s?",
    r"\d{1,2}\s*(?:st|nd|rd|th)\s*century",
    r".*circa.*",
    r".*[pP]eriod(?!.)"
]
SCDL_FIX_FORMAT = {
    "Pamphlet": "Pamphlets",
    "Pamplet": "Pamphlets",
    "Pamplets": "Pamphlets",
    "Pamplets\n": "Pamphlets",
    "pamphlets": "Pamphlets",
    "Manuscript": "Manuscripts",
    "manuscripts": "Manuscripts",
    "Photograph": "Photographs",
    "Baskets (Containers)": "Baskets (containers)",
    "color print": "Color prints",
    "color prints": "Color prints",
    "Image": "Images",
    "Manuscript": "Manuscripts",
    "Masks (costume)": "Masks (costumes)",
    "Newspaper": "Newspapers",
    "Object": "Objects",
    "Still image": "Still images",
    "Still Image": "Still images",
    "StillImage": "Still images",
    "Text\nText": "Texts",
    "Text": "Texts",
    "Batik": "Batiks",
    "Book": "Books",
    "Map": "Maps",
    "maps": "Maps",
    "Picture Postcards": "Picture postcards",
    "Religous objects": "Religious objects",
    "Tray": "Trays",
    "Wall hanging": "Wall hangings",
    "Wood carving": "Wood carvings",
    "Woodcarving": "Wood carvings",
    "Cartoons (humourous images)": "Cartoons (humorous images)",
    "black-and-white photographs": "Black-and-white photographs"
}


class Record(object):
    def __init__(self, col_id, record):
        self.collection_id = col_id
        self.source_metadata = record

    # Mapper Helpers
    def collate_subfield(self, field, subfield):
        return [f[subfield] for f in self.source_metadata.get(field, [])]

    # Enrichments
    def enrich(self, enrichment_function, **kwargs):
        func = getattr(self, enrichment_function)
        return func(**kwargs)

    def select_id(self, prop):
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
                RIGHTS_STATUS.get(collection.get('rights_status')),
                collection.get('rights_statement')
            ]
            rights = [r for r in rights if r]
            if not rights:
                rights = [RIGHTS_STATEMENT_DEFAULT]
            field_value = rights

        if field == "type":
            field_value = [DCMI_TYPES.get(collection.get('dcmi_type'), None)]

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
            for pattern in REGSEARCH:
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
        SCDL_FIX_FORMAT is right in the lookup.py file, though.

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
            substitution_dict = SCDL_FIX_FORMAT
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

    def filter_fields(self, keys):
        """
        called with the following parameters:
        2333 times: keys=["sourceResource"]

        TODO: this recursed in the dpla-ingestion codebase.
        afaik, we're not implementing a deeply nested data structure
        so I'm not implementing recursion here.
        """
        for key, val in self.mapped_data.items():
            if not val:
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
