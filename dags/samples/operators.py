from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sample"],
)
def sample_operators():

    @task()
    def taskflow_func():
        print("hello from taskflow task")

    def python_operator_callable():
        print("and hello from a python operator task")
    python_op = PythonOperator(
        task_id="python_operator",
        python_callable=python_operator_callable,
    )

    @task.docker(
        image="python:3",
        auto_remove=True,
        container_name="docker_decorated_task"
    )
    def docker_decorated_func():
        print("and hello from a python:3 docker decorated task")

    docker_op = DockerOperator(
        task_id="docker_operator",
        image="centos:latest",
        command="echo 'hello from docker operator task'",
        docker_url="unix://var/run/docker.sock",  # Set your docker URL
        network_mode="bridge",
        auto_remove="force",
    )

    # a sample chaining together the taskflow model w/ the operator model
    taskflow_func() >> python_op >> docker_decorated_func() >> docker_op

sample_operators()
