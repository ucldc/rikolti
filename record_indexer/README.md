## Create OpenSearch index template

To create the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html) for rikolti:

```
python index_templates/rikolti_template.py
```

This creates a template that will be used whenever an index with name matching `rikolti*` is added to the cluster.

## Run indexer from command line

Create a new index for a collection and add it to the `rikolti-stg` alias:

```
python -m record_indexer.create_collection_index <collection_id>
```

Add the current stage index for a collection to the `rikolti-prd` alias:

```
python -m record_indexer.move_index_to_prod <collection_id>
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






