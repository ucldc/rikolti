from django.shortcuts import render
import urllib.request
import json
import os
from elasticsearch import Elasticsearch
from django.conf import settings

es_host = settings.ES_HOST
es_user = settings.ES_USER
es_pass = settings.ES_PASS

# password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
# password_mgr.add_password(None, es_host, es_user, es_pass)
# handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
# opener = urllib.request.build_opener(handler)
# urllib.request.install_opener(opener)

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
		data = { "query": { } }
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
		hit['source'] = [{'term': k,'value':v, 'type':type(v).__name__}
			for k,v in hit['_source'].items() if k in fields]
		del(hit['_source'])
		hit['source'].sort(key=lambda k: fields.index(k['term']))
		hit['id'] = hit['_id']

	return render(request, 'rikolti/index.html', {
		'fields': fields, 
		'count': count,
		'search_results': hits,
		'search_string': request.GET.get('q', ''),
	})
