import boto3
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


@dag(
    dag_id="dev_content_harvester",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["dev"],
)
def dev_content_harvest():
    # get private subnets and security group from cloudformation stack
    client = boto3.client('cloudformation', region_name='us-west-2')
    private_subnets = []
    security_group = []
    cf_outputs = (client
                        .describe_stacks(StackName='pad-airflow-mwaa')
                        .get('Stacks', [{}])[0]
                        .get('Outputs', [])
    )
    for output in cf_outputs:
        if output['OutputKey'] in ['PrivateSubnet1', 'PrivateSubnet2']:
            private_subnets.append(output['OutputValue'])
        if output['OutputKey'] == 'SecurityGroup':
            security_group.append(output['OutputValue'])

    dev_content_harvest_task = EcsRunTaskOperator(
        task_id="content_harvest_ecs",
        cluster="rikolti-ecs-cluster",
        launch_type="FARGATE",
        platform_version = "LATEST",
        task_definition="rikolti-content_harvester-task-definition",
        overrides={
            "containerOverrides": [
                {
                    "name": "rikolti-content_harvester",
                    "command": ["3433", "0"],
                }
            ]
        },
        region="us-west-2",
        network_configuration = {
            "awsvpcConfiguration": {
                "subnets": private_subnets,
                "securityGroups": security_group,
                "assignPublicIp": "ENABLED",
            }
        },
        awslogs_group="rikolti-content_harvester",
        awslogs_region="us-west-2",
        awslogs_stream_prefix="ecs/rikolti-content_harvester",
        reattach=True,
        number_logs_exception=100,
    )

    dev_content_harvest_task


dev_content_harvest()