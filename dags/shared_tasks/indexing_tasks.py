from airflow.decorators import task

from rikolti.record_indexer.create_collection_index import create_index_name
from rikolti.record_indexer.create_collection_index import create_new_index
from rikolti.record_indexer.create_collection_index import delete_index
from rikolti.record_indexer.move_index_to_prod import move_index_to_prod

@task(task_id="create_stage_index")
def create_stage_index_task(collection: dict, version_pages: list[str]):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    index_name = create_index_name(collection_id)
    try:
        create_new_index(collection_id, version_pages, index_name)
    except Exception as e:
        delete_index(index_name)
        raise e
    return index_name


@task(task_id="move_index_to_prod")
def move_index_to_prod_task(collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    move_index_to_prod(collection_id)
