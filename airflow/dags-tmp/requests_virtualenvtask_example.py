from airflow.decorators import task, dag
from datetime import datetime, timedelta


@task.virtualenv(
    task_id="request_google", 
    requirements=["requests"], 
    system_site_packages=True
)
def callable_virtualenv():
    import requests
    print('yo this is whackadoodle')
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
def virtualenv_task_strat():
    virtualenv_task = callable_virtualenv()


virtualenv_task_strat()
