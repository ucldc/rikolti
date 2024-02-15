import json
from typing import Optional

from airflow.decorators import task, task_group

from rikolti.utils.versions import get_version
from rikolti.utils.versions import create_with_content_urls_version
from rikolti.dags.shared_tasks.content_harvest_operators import ContentHarvestOperator

@task()
def create_with_content_urls_version_task(
    collection: dict, mapped_page_batches: list[str]):
    mapped_page_batch = json.loads(mapped_page_batches[0])
    mapped_version = get_version(collection['id'], mapped_page_batch[0])
    return create_with_content_urls_version(mapped_version)


@task_group(group_id='content_harvesting')
def content_harvesting_tasks(
    collection: Optional[dict] = None, 
    mapped_page_batches: Optional[list[list[str]]] = None):

    with_content_urls_version = create_with_content_urls_version_task(
        collection, mapped_page_batches)

    content_harvest_task = (
        ContentHarvestOperator
            .partial(
                task_id="content_harvest", 
                collection_id="{{ params.collection_id }}",
                with_content_urls_version=with_content_urls_version,
                mapper_type=collection['rikolti_mapper_type']
            )
            .expand(
                pages=mapped_page_batches
            )
    )
    return with_content_urls_version, content_harvest_task
