from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

import os
import pwd
import grp
import stat
import docker

@task()
def get_user_and_group():
    print('USER')
    uid = os.geteuid()              # effective user ID of current process
    print(pwd.getpwuid(uid))

    print('GROUP')
    gid = os.getegid()             # effective group ID of current process
    print(grp.getgrgid(gid)) # Get group for given group ID

    print('PROCESS')
    pid = os.getpid()
    print(pid)                      # process ID of current process

    print('SESSION')
    print(os.getsid(pid))           # Get the session ID of the process with the given PID

@task()
def inspect_socket_permissions():
    # Get the file permissions of the socket file
    mode = os.stat("/var/run/docker.sock").st_mode

    # Check if it's a socket file
    if stat.S_ISSOCK(mode):
        # Extract the permission bits
        permissions = {
            'user_read': bool(mode & stat.S_IRUSR),
            'user_write': bool(mode & stat.S_IWUSR),
            'user_execute': bool(mode & stat.S_IXUSR),
            'group_read': bool(mode & stat.S_IRGRP),
            'group_write': bool(mode & stat.S_IWGRP),
            'group_execute': bool(mode & stat.S_IXGRP),
            'other_read': bool(mode & stat.S_IROTH),
            'other_write': bool(mode & stat.S_IWOTH),
            'other_execute': bool(mode & stat.S_IXOTH),
        }

        for key, value in permissions.items():
            print(f"{key}: {value}")
        return "Socket file exists"
    else:
        return "Not a socket file"

@task()
def is_docker_running():
    client = docker.from_env()
    client.ping()
    return True

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["sample"],
)
def docker_debug():

    docker_operator_task = DockerOperator(
        task_id="docker_operator_task",
        image="python:3.9.18-slim-bullseye",
        container_name="docker_operator_task",
        command="python -c 'print(\"hello from docker operator python\")'",
        network_mode="bridge",
        auto_remove='force'
    )
    (
        get_user_and_group() >> 
        inspect_socket_permissions() >> 
        is_docker_running() >> 
        docker_operator_task
    )

docker_debug()