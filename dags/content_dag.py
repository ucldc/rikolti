import os

from datetime import datetime
from docker.types import Mount
from airflow.decorators import dag
from airflow.models.param import Param

from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    dag_id="content_harvest",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(3433, description="Collection ID to harvet_content")},
    tags=["rikolti"],
)
def content_harvest():
    mounts = []
    if os.environ.get("CONTENT_DATA_MOUNT"):
        mounts.append(Mount(
            source=os.environ.get("CONTENT_DATA_MOUNT"),
            target="/rikolti_data",
            type="bind",
        ))
    if os.environ.get("CONTENT_MOUNT"):
        mounts.append(Mount(
            source=os.environ.get("CONTENT_MOUNT"),
            target="/rikolti_content",
            type="bind",
        ))
    if not mounts:
        mounts=None

    content_harvester_task = DockerOperator(
        task_id="content_harvester",
        image="content_harvester:latest",
        container_name="content_harvest_dag_task",
        command=["{{ params.collection_id }}"],
        network_mode="bridge",
        auto_remove='force',
        mount_tmp_dir=False,
        mounts=mounts,
        environment={
            "CONTENT_DATA_SRC": os.environ.get("CONTENT_DATA_SRC"),
            "CONTENT_DATA_DEST": os.environ.get("CONTENT_DATA_DEST"),
            "CONTENT_DEST": os.environ.get("CONTENT_DEST"),
            "NUXEO": os.environ.get("NUXEO"),
        }
    )

    content_harvester_task

content_harvest()
