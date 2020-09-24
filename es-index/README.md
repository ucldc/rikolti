## Sample ElasticSearch Queries

Get all docs:

```
$ curl -XGET -u pachamama-es-dev -p "https://search-pachamama-dev-public-w7mykn7bucrpqksnm7tu3vg3za.us-east-1.es.amazonaws.com/_search?pretty” -H "content-type: application/json" -d'
{
    "query": {
        "match_all": {}
    }
}
'
```

Count all docs:

```
curl -X GET -u pachamama-es-dev -p "https://search-pachamama-dev-public-w7mykn7bucrpqksnm7tu3vg3za.us-east-1.es.amazonaws.com/_cat/count"
```

Search the `testing` index for the term `women`:

```
$ curl -XGET -u pachamama-es-dev -p 'https://search-pachamama-dev-public-w7mykn7bucrpqksnm7tu3vg3za.us-east-1.es.amazonaws.com/testing/_search?q=women&pretty=true'
```

Documentation: https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/
