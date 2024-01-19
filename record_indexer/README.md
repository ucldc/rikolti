## Create OpenSearch index template

To create the [index template](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/index-templates.html) for rikolti:

```
python index_templates/rikolti_template.py
```

This creates a template that will be used whenever an index with name matching `rikolti*` is added to the cluster.

## Run indexer

Create a new index for a collection and add it to the `rikolti-stg` alias:

```
python -m record_indexer.create_collection_index <collection_id>
```

Add the current stage index for a collection to the `rikolti-prd` alias:

```
python -m record_indexer.move_index_to_prod <collection_id>
```

Note that the index creation code enforces the existence of one stage index at a time. This means we can simply supply the collection ID as input and the process will move the current stage index to production.









