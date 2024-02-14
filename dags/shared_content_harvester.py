import os
import boto3
import json

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from docker.types import Mount

# generally speaking, the CONTENT_HARVEST_EXECUTION_ENVIRONMENT should always
# be 'ecs' in deployed MWAA and should always be 'docker' in local development.
# if one is working to debug airflow's connection to ecs, though, it may be 
# useful to set CONTENT_HARVEST_EXECUTION_ENVIRONMENT to 'ecs' locally. 
CONTAINER_EXECUTION_ENVIRONMENT = os.environ.get(
    'CONTAINER_EXECUTION_ENVIRONMENT')


def get_awsvpc_config():
    """ 
    get private subnets and security group from cloudformation stack for use
    with the ContentHarvestEcsOperator to run tasks in an ECS cluster
    """
    client = boto3.client('cloudformation', region_name='us-west-2')
    awsvpcConfig = {
        "subnets": [],
        "securityGroups": []
    }
    cf_outputs = (client
                        .describe_stacks(StackName='pad-airflow-mwaa')
                        .get('Stacks', [{}])[0]
                        .get('Outputs', [])
    )
    for output in cf_outputs:
        if output['OutputKey'] in ['PrivateSubnet1', 'PrivateSubnet2']:
            awsvpcConfig['subnets'].append(output['OutputValue'])
        if output['OutputKey'] == 'SecurityGroup':
            awsvpcConfig['securityGroups'].append(output['OutputValue'])
    return awsvpcConfig


class ContentHarvestEcsOperator(EcsRunTaskOperator):
    def __init__(self, collection_id=None, with_content_urls_version=None, pages=None, mapper_type=None, **kwargs):
        container_name = "rikolti-content_harvester"

        args = {
            "cluster": "rikolti-ecs-cluster",
            "launch_type": "FARGATE",
            "platform_version": "LATEST",
            "task_definition": "rikolti-content_harvester-task-definition",
            "overrides": {
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": [
                            f"{collection_id}",
                            pages,
                            with_content_urls_version,
                            mapper_type
                        ],
                        "environment": [
                            {
                                "name": "MAPPED_DATA",
                                "value": "s3://rikolti-data"
                            },
                            {
                                "name": "WITH_CONTENT_URL_DATA",
                                "value": "s3://rikolti-data"
                            },
                            {
                                "name": "CONTENT_ROOT",
                                "value": "s3://rikolti-content"
                            },
                            {
                                "name": "NUXEO_USER",
                                "value": os.environ.get("NUXEO_USER")
                            },
                            {
                                "name": "NUXEO_PASS",
                                "value": os.environ.get("NUXEO_PASS")
                            }
                        ]
                    }
                ]
            },
            "region": "us-west-2",
            "awslogs_group": "rikolti-content_harvester",
            "awslogs_region": "us-west-2",
            "awslogs_stream_prefix": "ecs/rikolti-content_harvester",
            "reattach": True,
            "number_logs_exception": 100,
        }
        args.update(kwargs)
        super().__init__(**args)

    def execute(self, context):
        # Operators are instantiated once per scheduler cycle per airflow task
        # using them, regardless of whether or not that airflow task actually
        # runs. The ContentHarvestEcsOperator is used by ecs_content_harvester
        # DAG, regardless of whether or not we have proper credentials to call
        # get_awsvpc_config(). Adding network configuration here in execute
        # rather than in initialization ensures that we only call
        # get_awsvpc_config() when the operator is actually run.
        self.network_configuration = {
            "awsvpcConfiguration": get_awsvpc_config()
        }
        return super().execute(context)


class ContentHarvestDockerOperator(DockerOperator):
    def __init__(self, collection_id, with_content_urls_version, pages, mapper_type, **kwargs):
        mounts = []
        if os.environ.get("METADATA_MOUNT"):
            mounts.append(Mount(
                source=os.environ.get("METADATA_MOUNT"),
                target="/rikolti_data",
                type="bind",
            ))
        if os.environ.get("CONTENT_MOUNT"):
            mounts.append(Mount(
                source=os.environ.get("CONTENT_MOUNT"),
                target="/rikolti_content",
                type="bind",
            ))
        if os.environ.get("MOUNT_CODEBASE"):
            mounts = mounts + [
                Mount(
                    source=f"{os.environ.get('MOUNT_CODEBASE')}/content_harvester",
                    target="/content_harvester",
                    type="bind"
                ),
                Mount(
                    source=f"{os.environ.get('MOUNT_CODEBASE')}/utils",
                    target="/rikolti/utils",
                    type="bind"
                )
            ]
        if not mounts:
            mounts=None

        container_image = os.environ.get(
            'CONTENT_HARVEST_IMAGE',
            'public.ecr.aws/b6c7x7s4/rikolti/content_harvester'
        )
        container_version = os.environ.get(
            'CONTENT_HARVEST_VERSION', 'latest')

        json_pages = json.loads(pages)
        page_basename = json_pages[0].split('/')[-1]
        container_name = (
            f"content_harvester_{collection_id}_{page_basename.split('.')[0]}")

        if os.environ.get('MAPPED_DATA', '').startswith('s3'):
            mapped_data = os.environ.get('MAPPED_DATA')
        else:
            mapped_data = "file:///rikolti_data"

        if os.environ.get('WITH_CONTENT_URL_DATA', '').startswith('s3'):
            with_content_url_data = os.environ.get('WITH_CONTENT_URL_DATA')
        else:
            with_content_url_data = "file:///rikolti_data"

        if os.environ.get('CONTENT_ROOT', '').startswith('s3'):
            content_root = os.environ.get('CONTENT_ROOT')
        else:
            content_root = "file:///rikolti_content"

        args = {
            "image": f"{container_image}:{container_version}",
            "container_name": container_name,
            "command": [
                f"{collection_id}",
                pages,
                with_content_urls_version,
                mapper_type
            ],
            "network_mode": "bridge",
            "auto_remove": 'force',
            "mounts": mounts,
            "mount_tmp_dir": False,
            "environment": {
                "MAPPED_DATA": mapped_data,
                "WITH_CONTENT_URL_DATA": with_content_url_data,
                "CONTENT_ROOT": content_root,
                "NUXEO_USER": os.environ.get("NUXEO_USER"),
                "NUXEO_PASS": os.environ.get("NUXEO_PASS")
            },
            "max_active_tis_per_dag": 4
        }
        args.update(kwargs)
        super().__init__(**args)

    def execute(self, context):
        print(f"Running {self.command} on {self.image} image")
        return super().execute(context)

if CONTAINER_EXECUTION_ENVIRONMENT == 'ecs':
    ContentHarvestOperator = ContentHarvestEcsOperator
else:
    ContentHarvestOperator = ContentHarvestDockerOperator
