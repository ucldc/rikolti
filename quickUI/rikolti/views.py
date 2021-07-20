import json
import boto3
from json import JSONDecodeError
from botocore.exceptions import NoCredentialsError

from django.conf import settings
from django.shortcuts import render
from elasticsearch import Elasticsearch


es_host = settings.ES_HOST
es_user = settings.ES_USER
es_pass = settings.ES_PASS

# password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
# password_mgr.add_password(None, es_host, es_user, es_pass)
# handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
# opener = urllib.request.build_opener(handler)
# urllib.request.install_opener(opener)

s3_client = boto3.client('s3')

elastic_client = Elasticsearch(hosts=[es_host], http_auth=(es_user, es_pass))

# Create your views here.
def index(request):
    get = request.GET.copy()
    data = {
        "query": {
            "match_all": {}
        }
    }
    if 'q' in get:
        if len(get.get('q')) > 0:
            data = {
                "query": {
                    "query_string": {
                        "query": request.GET.get('q')
                    }
                }
            }
        del get['q']
    if len(get) > 0:
        data = {"query": {}}
        for term, value in get.items():
            data['query']['term'] = {term: value}

    data['highlight'] = {"fields": {"word_bucket": {}}}
    data['_source'] = {"includes": ["type", "title"]}
    # data = urllib.parse.urlencode(query)
    # data = data.encode('ascii')

    # mapping = elastic_client.indices.get_mapping("testing")
    # print(mapping)

    # with urllib.request.urlopen(f"{es_host}/_search?pretty", data.encode()) as resp:
    # hits = json.loads(resp.read())['hits']['hits']
    resp = elastic_client.search(index="testing", body=data, size=999)
    count = resp['hits']['total']['value']
    hits = resp['hits']['hits']

    fields = [
        'title', 
        'type', 
        'snippets'
        # 'source', 
        # 'date', 
        # 'publisher', 
        # 'transcription',
        # 'rights',
        # 'subject',
        # 'alternative_title', 
        # 'extent',
        # 'temporal',
        # 'provenance',
        # 'location',
    ]

    for hit in hits:
        hit['source'] = [{'term': k, 'value': v, 'type': type(v).__name__}
                         for k, v in hit['_source'].items() if k in fields]
        del(hit['_source'])
        hit['source'].sort(key=lambda k: fields.index(k['term']))
        hit['id'] = hit['_id']

    return render(request, 'rikolti/index.html', {
        'fields': fields, 
        'count': count,
        'search_results': hits,
        'search_string': request.GET.get('q', ''),
    })


def s3_search(item_id, collection_id, prefix):
    try:
        s3_resp = s3_client.list_objects_v2(
            Bucket='rikolti',
            Prefix=f'{prefix}{collection_id}/'
        )
    except NoCredentialsError as e:
        print(e)
        return ''

    keys = [obj['Key'] for obj in s3_resp['Contents']]
    found = ''
    for key in keys:
        s3_obj = s3_client.get_object(Bucket='rikolti', Key=key)
        s3_body = s3_obj['Body'].read().decode('utf-8')
        metadata_records = s3_body.split('\n')
        for metadata_record in metadata_records:
            try:
                json_metadata = json.loads(metadata_record)
                if json_metadata['calisphere-id'].split('--')[1] == item_id:
                    found = json_metadata
                    break
            except JSONDecodeError as e:
                print(e)
                print(key)
                print(s3_body)
        if found:
            break
    return found


def item_view(request, item_id):
    resp = elastic_client.get(index="testing", id=item_id)
    item = resp['_source']
    calisphere_link = item['calisphere-id'].split('--')[1]
    collection_id = item['calisphere-id'].split('--')[0]
    
    mapped_metadata = s3_search(
        calisphere_link, 
        collection_id, 
        'mapped_metadata/collection_id='
    )
    vernacular_metadata = s3_search(
        calisphere_link, 
        collection_id, 
        'vernacular_metadata/'
    )

    return render(request, 'rikolti/itemView.html', {
        'item_id': item_id,
        'calisphere_link': f'https://calisphere.org/item/{calisphere_link}/',
        'item': item,
        'item_json': json.dumps(item, indent=4),
        'mapped_metadata': json.dumps(mapped_metadata, indent=4),
        'vernacular_metadata': json.dumps(vernacular_metadata, indent=4)
    })
