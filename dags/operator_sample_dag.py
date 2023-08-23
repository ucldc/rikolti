from datetime import datetime
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
import requests

def requests_test(collection_id):
    print(f"collection_id: {collection_id}")
    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikolticollection/{collection_id}/"
    )
    resp.raise_for_status()
    print(resp.json().get('name'))

    return True

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(1, description="Collection ID")},
    tags=["test"],
)
def testing_dag():
    test_requests = PythonOperator(         # noqa: F841
        task_id="test_requests",
        python_callable=requests_test,
        op_kwargs={
            "collection_id": "{{ dag_run.conf['collection_id'] }}"
        }
    )

testing_dag()
