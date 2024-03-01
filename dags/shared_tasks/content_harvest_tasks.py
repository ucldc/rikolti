import json
from typing import Optional

from airflow.decorators import task, task_group

from rikolti.dags.shared_tasks.shared import notify_rikolti_failure
from rikolti.dags.shared_tasks.shared import send_log_to_sns
from rikolti.utils.versions import get_version
from rikolti.utils.versions import create_with_content_urls_version
from rikolti.dags.shared_tasks.content_harvest_operators import ContentHarvestOperator

@task(task_id="create_with_content_urls_version", 
      on_failure_callback=notify_rikolti_failure)
def create_with_content_urls_version_task(
    collection: dict, mapped_page_batches: list[str], **context):
    mapped_page_batch = json.loads(mapped_page_batches[0])
    mapped_version = get_version(collection['id'], mapped_page_batch[0])
    with_content_urls_version = create_with_content_urls_version(mapped_version)
    send_log_to_sns(
        context, {'with_content_urls_version': with_content_urls_version})
    return with_content_urls_version


def notify_content_harvest_success(context):
    send_log_to_sns(context, {'content_harvest_success': True})


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
                mapper_type=collection['rikolti_mapper_type'],
                on_failure_callback=notify_rikolti_failure,
                on_success_callback=notify_content_harvest_success
            )
            .expand(
                pages=mapped_page_batches
            )
    )
    return with_content_urls_version, content_harvest_task
