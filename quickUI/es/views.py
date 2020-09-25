from django.shortcuts import render
import urllib.request
import json
import os
from elasticsearch import Elasticsearch

es_host = os.environ['ES_HOST']
es_user = os.environ['ES_USER']
es_pass = os.environ['ES_PASS']

# password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
# password_mgr.add_password(None, es_host, es_user, es_pass)
# handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
# opener = urllib.request.build_opener(handler)
# urllib.request.install_opener(opener)

elastic_client = Elasticsearch(hosts=[es_host], http_auth=(es_user, es_pass))

# Create your views here.
def index(request):
	data = {
		"query": {
			"match_all": {}
		}
	}
	if request.GET.get('q'):
		data = {
  			"query": {
    			"query_string": {
      				"query": request.GET.get('q')
    			}
  			}
		}

	# data = urllib.parse.urlencode(query)
	# data = data.encode('ascii')

	# with urllib.request.urlopen(f"{es_host}/_search?pretty", data.encode()) as resp:
		# hits = json.loads(resp.read())['hits']['hits']
	resp = elastic_client.search(body=data, size=999)
	count = resp['hits']['total']['value']
	hits = resp['hits']['hits']

	fields = [
		'title', 
		'type', 
		'source', 
		'date', 
		'publisher', 
		'transcription',
		'rights',
		'subject',
		'alternative_title', 
		'extent',
		'temporal',
		'provenance',
		'location',
	]

	for hit in hits:
		hit['source'] = [(k,v) for k,v in hit['_source'].items() if k in fields]
		del(hit['_source'])
		hit['source'].sort(key=lambda k: fields.index(k[0]))
		hit['id'] = hit['_id']

	return render(request, 'es/index.html', {
		'fields': fields, 
		'count': count,
		'search_results': hits,
		'search_string': request.GET.get('q', ''),
	})
