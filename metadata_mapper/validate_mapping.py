import sys
import os
import boto3
import requests
import json
import settings

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def local_path(folder, collection_id):
    parent_dir = os.sep.join(os.getcwd().split(os.sep)[:-1])
    local_path = os.sep.join([
        parent_dir,
        'rikolti_bucket',
        folder,
        str(collection_id),
    ])
    return local_path


def solr(**params):
    solr_url = f'{settings.SOLR_URL}/query/'
    solr_auth = {'X-Authentication-Token': settings.SOLR_API_KEY}
    query = {}
    for key, value in list(params.items()):
        key = key.replace('_', '.')
        query.update({key: value})
    res = requests.post(solr_url, headers=solr_auth, data=query, verify=False)
    res.raise_for_status()
    results = json.loads(res.content.decode('utf-8'))
    # facet_counts = results.get('facet_counts', {})
    return results


def list_of_strings(l):
    return isinstance(l, list) and all(isinstance(s, str) for s in l)


valid_types = [
    "collection",
    "dataset",
    "event",
    "image",
    "interactive resource",
    "moving image",
    "physical object",
    "service",
    "software",
    "sound",
    "text",
]

full_fidelity_fields = [
    {
        'field': 'id',
        'type': lambda ark: isinstance(ark, str),
        'validation': lambda ark: ark.startswith("ark:/")
    },
    {'field': 'identifier', 'type': list_of_strings},
    {'field': 'title', 'type': list_of_strings},
    {
        'field': 'type',
        'type': list_of_strings,
        'validation': lambda t: t[0] in valid_types and len(t) == 1
    },
    {'field': 'rights', 'type': list_of_strings},
    {
        'field': 'rights_uri',
        'type': list_of_strings,
        'validation': lambda r: len(r) == 1
    }
]

partial_fidelity_fields = [
    "alternative_title",
    "contributor",
    "coverage",
    "creator",
    "date",
    "description",
    "extent",
    "format",
    "genre",
    "language",
    "location",
    "provenance",
    "publisher",
    "relation",
    "rights_holder",
    "rights_note",
    "rights_date",
    "source",
    "spatial",
    "subject",
    "temporal",
    "transcription",
]

repository_fields = [
    'campus_data',
    'campus_name',
    'campus_url',
    'collection_data',
    'collection_name',
    'collection_url',
    'repository_data',
    'repository_name',
    'repository_url',
    'sort_collection_data',
]

excluded_fields = [
    'reference_image_md5',
    'reference_image_dimensions',
    'structmap_url',
    'url_item',
    'harvest_id_s'
]

search_fields = [
    'facet_decade',
    'sort_date_end',
    'sort_date_start',
    'sort_title',
]

harvest_fields = [
    '_version_',
    'harvest_id_s',
    'timestamp',
]

enrichment_fields = (
    repository_fields +
    excluded_fields +
    search_fields +
    harvest_fields
)


def validate_mapped_page(rikolti_records, solr_records, query):
    collections_search = solr(**query)
    num_solr_records = collections_search['response']['numFound']
    query['rows'] = 100
    report = []

    while len(rikolti_records) and query['start'] < num_solr_records:
        print(
            f"records remaining on this page: {len(rikolti_records)} "
            f"vs. records fetched from solr: {len(solr_records)}"
        )
        print(query)
        collections_search = solr(**query)
        collections_search = collections_search.get(
            'response', {'docs': None}).get('docs', [])
        solr_records.update(
            {r['harvest_id_s']: r for r in collections_search})

        page_intersection = list(
            set(rikolti_records.keys()).intersection(solr_records.keys())
        )

        while page_intersection:
            harvest_id = page_intersection.pop(0)
            rikolti_record = rikolti_records.pop(harvest_id)
            solr_record = solr_records.pop(harvest_id)
            for field in full_fidelity_fields:
                field_name = field.get('field')
                field_type = field.get('type')
                field_validation = field.get('validation')

                rikolti_field = rikolti_record.get(field_name, None)
                solr_field = solr_record.get(field_name, None)

                if rikolti_field != solr_field:
                    report.append(f"ERROR: {harvest_id} {field_name} mismatch")
                    report.append(
                        f"{harvest_id}, {field_name}, "
                        f"{rikolti_field}, {solr_field}"
                    )

                field_type_check = True
                if field_type:
                    if not field_type(rikolti_field):
                        field_type_check = False
                        report.append(
                            f"[ERROR: Invalid Type]: {harvest_id} has invalid "
                            f"{field_name} in rikolti record: {rikolti_field}"
                        )

                if field_validation and field_type_check:
                    if not field_validation(rikolti_field):
                        report.append(
                            f"[ERROR: Invalid]: {harvest_id} has invalid "
                            f"{field_name} in rikolti record: {rikolti_field}"
                        )
            for field in partial_fidelity_fields:
                rikolti_field = rikolti_record.get(field, None)
                solr_field = solr_record.get(field, None)
                if rikolti_field != solr_field:
                    report.append(f"ERROR: {harvest_id} {field_name} mismatch")
                    report.append(
                        f"{harvest_id}, {field_name}, "
                        f"{rikolti_field}, {solr_field}"
                    )

        query['start'] = query['start'] + 100

    return query, solr_records, report


    # print(
    #     "Rikolti Record Id | CouchDB Id | Field Name | Solr Value | "
    #     "Rikolti Value"
    # )
    
    # for rikolti_record in mapped_metadata:
    #     # TODO: really inefficient to query for each item individually;
    #     # could we filter by collection and retrieve first 100 items in solr?
    #     # would need to manage solr pagination against rikolti pagination,
    #     # no guarantee the order will be the same...but maybe it's close
    #     # enough to just take a greedy approach?
    #     # Also, as updates are made to the mapper to accomodate future
    #     # collections, we'd want to ensure that we aren't breaking any past
    #     # collections that already work. Again, it's inefficient to keep
    #     # hitting solr, but maybe this is an optimization for much later.

    #     query = {"q": rikolti_record['calisphere-id']}
    #     solr_record = solr(**query)

    #     rikolti_fields = [field for field in rikolti_record.keys()]

    #     for field, value in solr_record.items():
    #         if field in excluded_fields:
    #             continue

    #         if field[-3:] == "_ss":
    #             field = field[:-3]

    #         if field not in rikolti_record:
    #             print(
    #                 f"{rikolti_record['calisphere-id']} | "
    #                 f"{solr_record['harvest_id_s']} | "
    #                 f"{field} | {value} | "
    #                 f"NONE"
    #             )
    #             continue

    #         if field in rikolti_record and rikolti_record.get(field) != value:
    #             print(
    #                 f"{rikolti_record['calisphere-id']} | "
    #                 f"{solr_record['harvest_id_s']} | "
    #                 f"{field} | {value} | "
    #                 f"{rikolti_record[field]}"
    #             )
    #             rikolti_fields.pop(rikolti_fields.index(field))
    #             continue

    #     for field in rikolti_fields:
    #         print(
    #             f"{rikolti_record['calisphere-id']} | "
    #             f"{solr_record['harvest_id_s']} | "
    #             f"{field} | NONE | "
    #             f"{rikolti_record[field]}"
    #         )

    #     if (rikolti_record['calisphere-id'] ==
    #             'ccf27d23-738b-4b88-a1de-afeef5e9bda7'):
    #         break


def validate_mapped_collection(payload):
    payload = json.loads(payload)
    collection_id = payload.get('collection_id')

    if settings.DATA_SRC == 'local':
        mapped_path = local_path('mapped_metadata', collection_id)
        page_list = [f for f in os.listdir(mapped_path)
                     if os.path.isfile(os.path.join(mapped_path, f))]

    solr_records = {}
    query = {
        'fq': (
            "collection_url: \"https://registry.cdlib.org/api/v1"
            f"/collection/{collection_id}/\""
        ),
        'rows': 0,
        'start': 0
    }
    reports = []

    for page in page_list:
        if settings.DATA_SRC == 'local':
            page_path = os.sep.join([
                local_path('mapped_metadata', collection_id),
                str(page)
            ])
            page = open(page_path, "r")
            mapped_metadata = page.read()
        elif settings.DATA_SRC == 's3':
            s3 = boto3.resource('s3')
            bucket = 'rikolti'
            key = f"mapped_metadata/{collection_id}/{page}"
            s3_obj_summary = s3.Object(bucket, key).get()
            mapped_metadata = s3_obj_summary['Body'].read()

        rikolti_records = json.loads(mapped_metadata)
        rikolti_records = {
            f"{collection_id}--{r['calisphere-id']}": r for r in rikolti_records
        }

        query, solr_records, report = validate_mapped_page(
            rikolti_records, solr_records, query)
        reports = reports + report

    print(reports)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Validate mapped metadata against SOLR")
    parser.add_argument('payload', help='json payload')
    args = parser.parse_args(sys.argv[1:])
    validate_mapped_collection(args.payload)