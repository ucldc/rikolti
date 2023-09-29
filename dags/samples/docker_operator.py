from datetime import datetime
from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sample"],
)
def sample_docker_operators():
    
    docker_operator_task = DockerOperator(
        task_id="docker_operator_task", 
        image="simple_python:latest",
        container_name="simple_python_container_task",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove='force',
    )

    docker_operator_task

sample_docker_operators()
