from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from rikolti.record_indexer.create_collection_index import create_new_index
from rikolti.record_indexer.create_collection_index import delete_index
from rikolti.record_indexer.move_index_to_prod import move_index_to_prod

@task()
def create_stage_index_task(collection: dict, version_pages: list[str]):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    index_name = create_new_index(collection_id, version_pages)
    return index_name


# Task is triggered if at least one upstream (direct parent) task has failed
@task(trigger_rule=TriggerRule.ONE_FAILED)
def cleanup_failed_index_creation_task(index_name: str):
    delete_index(index_name)


@task()
def move_index_to_prod_task(collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    move_index_to_prod(collection_id)
