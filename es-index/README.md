## Sample ElasticSearch Queries

Get all docs:

```
$ curl -XGET -u rikolti-admin -p "https://search-rikolti-kqxwbatz7xfb6cvwmmafngcpsi.us-west-2.es.amazonaws.com/_search?pretty" -H "content-type: application/json" -d'
{
    "query": {
        "match_all": {}
    }
}
'
```

Count all docs:

```
$ curl -X GET -u rikolti-admin -p "https://search-rikolti-kqxwbatz7xfb6cvwmmafngcpsi.us-west-2.es.amazonaws.com/_cat/count"
```

Search the `testing` index for the term `women`:

```
$ curl -XGET -u rikolti-admin -p 'https://search-rikolti-kqxwbatz7xfb6cvwmmafngcpsi.us-west-2.es.amazonaws.com/testing/_search?q=women&pretty=true'
```

Documentation: https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/
