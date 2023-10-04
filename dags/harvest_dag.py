from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param


from rikolti.dags.shared_tasks import fetch_collection_task
from rikolti.dags.shared_tasks import get_collection_fetchdata_task
from rikolti.dags.shared_tasks import get_collection_metadata_task
from rikolti.dags.shared_tasks  import map_page_task
from rikolti.dags.shared_tasks  import get_mapping_status_task



@dag(
    dag_id="harvest_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to harvest")},
    tags=["rikolti"],
)
def harvest():

    # see TODO at get_collection_fetchdata_task
    fetchdata = get_collection_fetchdata_task()
    collection = get_collection_metadata_task()

    fetched_pages = fetch_collection_task(collection=fetchdata)
    mapped_pages = (
        map_page_task
            .partial(collection=collection)
            .expand(page=fetched_pages)
    )
    get_mapping_status_task(collection, mapped_pages)
    

harvest()