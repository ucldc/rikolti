import os
import json
import re

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
    "\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}",
    "\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}",
    "\d{4}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{4}\s*[-/]\s*\d{1,2}",
    "\d{1,4}\s*[-/]\s*\d{1,4}\s*[-/]\s*\d{1,4}",
    "\d{4}\s*[-/]\s*\d{4}",
    "\d{1,2}\s*[-/]\s*\d{4}",
    "\d{4}\s*[-/]\s*\d{1,2}",
    "\d{4}s?",
    "\d{1,2}\s*(?:st|nd|rd|th)\s*century",
    ".*circa.*",
    ".*[pP]eriod(?!.)"
]


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

    def required_values_from_collection_registry(self, collection, field, mode=None):
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
        field = field[0].split('/')[1:] # remove sourceResource
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
        1105 times: prop=["originalRecord/collection"],   to_prop=["dataProvider"]
        1000 times: prop=["provider/name"],               to_prop=["dataProvider"],
        675 times:  prop=["sourceResource/publisher"],    to_prop=["dataProvider"]    skip_if_exists=["True"]
        549 times:  prop=["provider/name"],               to_prop=["dataProvider"],   no_overwrite=["True"]   # no_overwrite not implemented in dpla-ingestion
        440 times:  prop=["provider/name"],               to_prop=["dataProvider"],   skip_if_exists=["true"]
        293 times:  prop=["sourceResource%4Fpublisher"],  to_prop=["dataProvider"]
        86 times:   prop=["provider/name"],               to_prop=["dataProvider"]
        11 times:   prop=["originalRecord/type"],         to_prop=["sourceResource/format"]
        1 times:    prop=["sourceResource/spatial"],      to_prop=["sourceResource/temporal"], skip_if_exists=["true"]
        1 times:    prop=["sourceResource/description"],  to_prop=["sourceResource/title"]
        """

        src = prop[0].split('/')[1:]
        dest = to_prop[0].split('/')[1:]
        skip_if_exists = bool(skip_if_exists[0] in ['True', 'true'])

        if ((dest in self.mapped_data and skip_if_exists) or
                (src not in self.mapped_data)):
            # dpla-ingestion code "passes" here, but I'm not sure what that
            # means - does it skip this enrichment or remove this record?
            return self

        src_value = self.mapped_data.get(src)
        dest_value = self.mapped_data.get(dest, [])
        if ((not (isinstance(src_value, list) or isinstance(src_value, str))) or
            (not (isinstance(dest_value, list) or isinstance(dest_value, str)))):
            print(
                f"Prop {src} is {type(src_value)} and prop {dest} is "
                f"{type(dest_value)} - not a string/list for record "
                f"{self.mapped_data.get('id')}"
            )
            return self

        if isinstance(src_value, str):
            src_value = [src_value]
        if isinstance(dest_value, str):
            dest_value = [dest_value]

        self.mapped_data[dest] = dest_value + src_value
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
            cleaned_value = re.sub("[\(\)\.\?]", "", value)
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
