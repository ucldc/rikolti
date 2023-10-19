import os

from datetime import datetime
from airflow.decorators import dag, task

@task()
def taskflow_mkdir():
    """ we have permissions inside /airflow/, but nowhere else it seems """
    if os.path.exists("/usr/local/airflow/rikolti_data/test_dir"):
        os.remove("/usr/local/airflow/rikolti_data/test_dir/test2.txt")
        os.rmdir("/usr/local/airflow/rikolti_data/test_dir")

    os.mkdir("/usr/local/airflow/rikolti_data/test_dir")
    with open("/usr/local/airflow/rikolti_data/test_dir/test2.txt", "w") as f:
        f.write("hi amy")
    return True

@task()
def taskflow_write_to_disk():
    """ write a file to disk """
    with open("/usr/local/airflow/rikolti_data/test.txt", "w") as f:
        f.write("hello world")
    return True

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sample"],
)
def sample_filesystem_management():
    taskflow_mkdir()
    taskflow_write_to_disk()

sample_filesystem_management()
