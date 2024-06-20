## Create OpenSearch index template

To create the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html) for rikolti:

Make sure that RIKOLTI_ES_ENDPOINT is set in your environment.

```
python -m record_indexer.index_templates.rikolti_template
```

This creates a template that will be used whenever an index with name matching `rikolti*` is added to the cluster.

## Run indexer from command line

Create a new index for a collection and add it to the `rikolti-stg` alias:

```
python -m record_indexer.create_collection_index <collection_id>
```

## Indexer development using aws-mwaa-local-runner

See the Rikolti README page section on [Airflow Development](https://github.com/ucldc/rikolti/#airflow-development). In particular, make sure that indexer-related env vars are set as described there.

## Index lifecycle

The lifecycle of an index is as follows:

#### Create new index
1. Create a new index named `rikolti-{collection_id}-{version}`, where `version` is the current datetime).
2. Remove any existing indices for the collection from the `rikolti-stg` alias.
3. Add the new index to the `rikolti-stg` alias.
4. Delete any older unaliased indices, retaining the number of unaliased indices specified by `settings.INDEX_RETENTION`.

Note that the index creation code enforces the existence of one stage index at a time.

#### Move staged index to production
1. Identify the current stage index for the collection.
2. Remove any existing indices for the collection from the `rikolti-prd` alias.
3. Add the current stage index to the `rikolti-prd` alias. (This means that at this stage in the lifecycle, the index will be aliased to `rikolti-stg` and `rikolti-prd` at the same time.)

#### Delete old index
This happens during index creation (see step 4. above).

## OpenSearch index record updates
An option for updating records that are already in the OpenSearch index is to use the [`update by query` API](https://opensearch.org/docs/latest/api-reference/document-apis/update-by-query/). For example, we wanted to update records in `rikolti-stg` to make the value of `type` lowercase. Here is the query that we issued to update `Text` to `text`. Since it was a quick one-off update, we simply issued this in the OpenSearch Dashboard API console:

```
POST rikolti-stg/_update_by_query
{
  "query": {
    "term": {
      "type.raw": "Text"
    }
  },
  "script": {
    "source": "ctx._source.type = ctx._source.type[0].toLowerCase()",
    "lang": "painless"
  }
}
```






