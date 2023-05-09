from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator


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
def simple_http_operator_strat(): 
    get_op = SimpleHttpOperator(
        task_id='request_google',
        method='GET',
        endpoint='http://google.com',
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: True if response.status_code == 200 else False,
    )

simple_http_operator_strat()
