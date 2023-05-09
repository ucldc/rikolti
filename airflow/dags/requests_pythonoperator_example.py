from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

import requests

# https://registry.cdlib.org/api/v1/rikoltifetcher/26224/?format=json

def py_callable():
    print('yo this is whackanooooooodle')
    resp = requests.get('http://google.com', timeout=5)
    print(resp.status_code)
    return True


@dag(
    default_args={
        'start_date': datetime(2023, 5, 8),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
    tags=['rikolti'],
)
def python_operator_strat():    
    old_task = PythonOperator(
        task_id="request_google",
        python_callable=py_callable
    )

python_operator_strat()
