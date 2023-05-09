from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

import requests


def py_callable():
    print('yo this is whackanooooooodle')
    http = requests.Session()
    print(http)
    req = requests.Request('GET', 'https://google.com')
    print(req)
    req = req.prepare()
    print(req)
    resp = http.send(req)
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
def python_operator_session_strat():
    old_task = PythonOperator(
        task_id="request_google_with_session",
        python_callable=py_callable
    )

python_operator_session_strat()
