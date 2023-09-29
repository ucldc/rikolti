from datetime import datetime
from docker.types import Mount
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'message': Param("hello from your dags parameters", description="message to print")},
    tags=["sample"],
)
def sample_docker_operators():
    
    basic_task = DockerOperator(
        task_id="basic_task",
        image="simple_python:latest",
        container_name="basic_task_container",
        network_mode="bridge",
        auto_remove='force',
    )
    basic_task

    specify_print_task = DockerOperator(
        task_id="specific_task",
        image="simple_python:latest",
        container_name="specific_task_container",
        command="--message 'there are cats in here'",
        network_mode="bridge",
        auto_remove='force',
    )
    specify_print_task

    parameterize_print_task = DockerOperator(
        task_id="parameterize_task",
        image="simple_python:latest",
        container_name="parameterize_task_container",
        command="--message '{{ params.message }}'",
        network_mode="bridge",
        auto_remove='force',
        mount_tmp_dir=False,
    )
    parameterize_print_task

    # https://docker-py.readthedocs.io/en/stable/api.html#docker.types.Mount
    # since the docker daemon is running on the host machine, we can mount
    # a folder from the host machine into the container, but we cannot mount
    # a folder from the airflow container machine into the container
    mount_folder_task = DockerOperator(
        task_id="mount_folder_task",
        image="simple_python:latest",
        container_name="mount_folder_task_container",
        command="--message '{{ params.message }}' --output /tmp/rikolti_data/output.txt",
        network_mode="bridge",
        auto_remove='force',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/Users/awieliczka/Projects/rikolti_data",
                target="/tmp/rikolti_data",
                type="bind",
            )
        ]
    )
    mount_folder_task

sample_docker_operators()
