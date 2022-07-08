# requires avram: github.com/ucldc/avram
from library_collection.models import Collection

def split_enrichment(c):
    return {
        "enrichment_string": c.enrichments_item,
        "enrichment_list": c.enrichments_item.split(',\r\n'),
        "harvest_type": c.harvest_type,
        "col_id": c.id,
        "collection": c,
    } 


def abstract_group_by(items, group_by):
    unique = []
    while(len(items) > 0):
        remainder = []
        matched = []
        group = group_by(items[0])
        for item in items:
            if group_by(item) == group:
                matched.append(item)
            else:
                remainder.append(item)
        if group in unique:
            print('ERROR')
        unique.append({
            "group": group,
            "matched": matched
        })
        items = remainder
    return unique


def get_rich_collections_by_harvest_type(harvest_type):
    collections = (Collection.objects
        .filter(harvest_type__exact=harvest_type)
        .exclude(ready_for_publication__exact=False)
        .exclude(enrichments_item__exact=''))
    rich_collections = [split_enrichment(c) for c in collections]
    return rich_collections


def enrichments_report(rich_collections, harvest_type):
    report = {}
    unique_enrichments = abstract_group_by(
        rich_collections, 
        lambda c: c["enrichment_string"])
    report['number of unique enrichments'] = len(unique_enrichments)

    if harvest_type == 'OAC':
        oac_slash_n = [i['col_id'] for i in first_items[1]['matched']]
        # len(oac_slash_n) == 415
        for rc in rich_collections:
            if rc['col_id'] in oac_slash_n:
                rc['enrichment_list'] = rc['enrichment_list'][0].split(',\n')

    first_items = abstract_group_by(
        rich_collections, 
        lambda c: c['enrichment_list'][0])
    report['number of first enrichments'] = len(first_items)
    report['first items'] = [i['group'] for i in first_items]

    second_items = abstract_group_by(
        rich_collections, 
        lambda c: c['enrichment_list'][1])
    report['number of second enrichments'] = len(second_items)
    report['second_items'] = [{i['group']: len(i['matched'])} for i in second_items]


def get_rich_collections_by_mapper_type(second_items, mapper_type):
    contentdm_collections = [
        i['matched'] for i in second_items if 
        i['group'] == f'/dpla_mapper?mapper_type={mapper_type}'
    ]
    contentdm_collections = contentdm_collections[0]
    return contentdm_collections

def make_harvest_payloads(rich_collections):
    return [{
        'collection_id': f"{i['col_id']}-oac-datel",
        'harvest_type': 'DatelOACFetcher',
        'write_page': 0,
        'oac': {
            'url': i['collection'].url_harvest
        }
    } for i in rich_collections]

rich_collections = get_rich_collections_by_harvest_type('OAC')
print(enrichments_report(rich_collections, 'OAC'))

rich_collections = get_rich_collections_by_harvest_type('OAI')
oai_report = (enrichments_report(rich_collections, 'OAI'))
print(oai_report)

contentdm_collections = get_rich_collections_by_mapper_type(
    oai_report['second_items'], 'contentdm_oai_dc')

oac_rikolti_harvest_payloads = make_harvest_payloads(
    rich_collections)


contentdm_rikolti_harvest_payloads = [{
    'collection_id': i['col_id'],
    'harvest_type': "OAIFetcher",
    'write_page': 0,
    'oai': {
        'url': i['collection'].url_harvest,
        'extra_data': i['collection'].harvest_extra_data
    }
} for i in contentdm_collections]

import boto3
aws_access_key=
aws_secret=
aws_session=

lambda_client = boto3.client(
    'lambda', 
    region_name='us-west-2', 
    aws_access_key_id=ACCESS_KEY, 
    aws_secret_access_key=SECRET, 
    aws_session_token=SESSION
)
print(lambda_client.list_functions())
print(oac_rikolti_harvest_payloads[0])

import json
lambda_client.invoke(
    FunctionName="fetch-metadata",
    InvocationType="Event",
    Payload=json.dumps(
        oac_rikolti_harvest_payloads[0]
    ).encode('utf-8')
)

invocations = [
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(contentdm_col).encode('utf-8')
    ) for contentdm_col in contentdm_rikolti_harvest_payloads[0:200]
]
invocations = [
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(oac_col).encode('utf-8')
    ) for oac_col in oac_rikolti_harvest_payloads[500:]
]
collection_ids = [c['collection_id'] for c in contentdm_rikolti_harvest_payloads[0:10]]
collection_ids = [c['collection_id'] for c in oac_rikolti_harvest_payloads[0:10]]
collection_ids = [c['collection_id'] for c in oac_rikolti_harvest_payloads]

print(collection_ids)

s3_client = boto3.client('s3', region_name='us-west-2', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET, aws_session_token=SESSION)
bucket = 'rikolti'
prefix = 'vernacular_metadata/'
s3_objs = s3_client.list_objects(
    Bucket=bucket, 
    Prefix=prefix, 
    Delimiter='/'
)
for o in s3_objs.get('CommonPrefixes'):
    print(o.get('Prefix'))

def get_fetched_collections(s3_client):
   s3_objs = s3_client.list_objects(Bucket='rikolti', Prefix='vernacular_metadata/', Delimiter='/')
   return [o.get('Prefix') for o in s3_objs.get('CommonPrefixes')]


first_set = [4425, 5401, 7955, 8235, 9844, 13762, 14885, 15063, 15934, 16161]
second_set = [16887, 16888, 16908, 17466, 18106, 18980, 19404, 19611, 23030, 24195]
jobs_started = first_set + second_set
jobs_started = [f"vernacular_metadata/{i}/" for i in jobs_started]
jobs_started = [f"vernacular_metadata/{i}/" for i in collection_ids]
jobs_finished = get_fetched_collections(s3_client)
set(jobs_started) - set(jobs_finished)
{
    'vernacular_metadata/15063/', 
    'vernacular_metadata/16888/', 
    'vernacular_metadata/7955/', 
    'vernacular_metadata/18980/', 
    'vernacular_metadata/13762/', 
    'vernacular_metadata/8235/', 
    'vernacular_metadata/24195/', 
    'vernacular_metadata/15934/', 
    'vernacular_metadata/9844/', 
    'vernacular_metadata/16908/', 
    'vernacular_metadata/19404/', 
    'vernacular_metadata/14885/'
}
>>> len(set(jobs_started) - set(jobs_finished))
12
>>> invocations = [
...     lambda_client.invoke(
...         FunctionName="fetch-metadata", 
...         InvocationType="Event", 
...         Payload=json.dumps(contentdm_col).encode('utf-8')
...     ) for contentdm_col in contentdm_rikolti_harvest_payloads[20:50]
... ]
>>> jobs_started = jobs_started + [f"vernacular_metadata/i['collection_id']/" for i in contentdm_rikolti_harvest_payloads[20:50]]
>>> len(jobs_started)
50
>>> len(set(jobs_started) - set(get_fetched_collections(s3_client)))
13
>>> jobs_started = jobs_started + [f"vernacular_metadata/{i['collection_id']}/" for i in contentdm_rikolti_harvest_payloads[50:100]]


for contentdm_col in contentdm_rikolti_harvest_payloads[100:110]:
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(contentdm_col).encode('utf-8')
    )
    print(contentdm_col['collection_id'])

for contentdm_col in contentdm_rikolti_harvest_payloads[110:150]:
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(contentdm_col).encode('utf-8')
    )
    print(contentdm_col['collection_id'])

for contentdm_col in contentdm_rikolti_harvest_payloads[150:250]:
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(contentdm_col).encode('utf-8')
    )
len(contentdm_rikolti_harvest_payloads)
286
for contentdm_col in contentdm_rikolti_harvest_payloads[250:286]:
    lambda_client.invoke(
        FunctionName="fetch-metadata", 
        InvocationType="Event", 
        Payload=json.dumps(contentdm_col).encode('utf-8')
    )
actually harvested 93 collections of 286. 
not sure what happened to remaining 193 collections 
