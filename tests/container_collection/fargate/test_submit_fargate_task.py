import os
import unittest
from unittest import mock

import boto3
from moto import mock_aws

from container_collection.fargate.submit_fargate_task import submit_fargate_task

ACCOUNT = "123123123123"
REGION = "default-region"
CLUSTER = "cluster-name"


@mock.patch.dict(
    os.environ,
    {
        "MOTO_ALLOW_NONEXISTENT_REGION": "True",
        "MOTO_ACCOUNT_ID": ACCOUNT,
        "AWS_DEFAULT_REGION": REGION,
    },
)
class TestSubmitFargateTask(unittest.TestCase):
    def setUp(self) -> None:
        # Note that the memory specified in the container definition is not
        # actually required for Fargate, but is used in the moto library to
        # determine resource requirements.
        self.task_definition = {
            "family": "task-definition-name",
            "containerDefinitions": [
                {
                    "name": "task-definition-name",
                    "image": "jobimage:latest",
                    "memory": 256,
                }
            ],
            "requiresCompatibilities": ["FARGATE"],
            "cpu": "1",
            "memory": "256",
        }

        arn_prefix = f"arn:aws:ecs:{REGION}:{ACCOUNT}"
        self.task_definition_arn = f"{arn_prefix}:task-definition/task-definition-name:1"
        self.cluster_arn = f"{arn_prefix}:cluster/{CLUSTER}"

    @mock_aws
    def test_submit_fargate_task(self):
        name = "task-name"
        user = "user"
        command = ["command", "string"]
        cluster = "cluster-name"

        initialize_infrastructure()
        ec2_client = boto3.client("ec2")
        ecs_client = boto3.client("ecs")
        ecs_client.register_task_definition(**self.task_definition)

        security_group_id = ec2_client.describe_security_groups()["SecurityGroups"][0]["GroupId"]
        subnet_id = ec2_client.describe_subnets()["Subnets"][0]["SubnetId"]

        task_arn = submit_fargate_task(
            name,
            self.task_definition_arn,
            user,
            cluster,
            [security_group_id],
            [subnet_id],
            command,
            launchType="FARGATE",
            capacityProviderStrategy=[],
        )

        response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
        task = response["tasks"][0]
        self.assertEqual(1, len(response["tasks"]))
        self.assertEqual(f"{user}_{name}", task["overrides"]["containerOverrides"][0]["name"])
        self.assertEqual(command, task["overrides"]["containerOverrides"][0]["command"])
        self.assertEqual(task_arn, task["taskArn"])
        self.assertEqual(self.cluster_arn, task["clusterArn"])
        self.assertEqual(self.task_definition_arn, task["taskDefinitionArn"])


def initialize_infrastructure() -> None:
    # Create clients.
    ec2_client = boto3.client("ec2")
    ecs_client = boto3.client("ecs")

    # Create VPC.
    vpc = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    # Create subnet.
    ec2_client.create_subnet(AvailabilityZone="us-east-1a", CidrBlock="10.0.0.0/18", VpcId=vpc_id)

    # Create security group.
    ec2_client.create_security_group(
        Description="test security group description",
        GroupName="security-group-name",
        VpcId=vpc_id,
    )

    # Create ECS Fargate cluster.
    ecs_client.create_cluster(
        clusterName=CLUSTER,
        capacityProviders=["FARGATE", "FARGATE_SPOT"],
        defaultCapacityProviderStrategy=[
            {"capacityProvider": "FARGATE_SPOT", "weight": 5, "base": 0},
            {"capacityProvider": "FARGATE", "weight": 5, "base": 0},
        ],
    )


if __name__ == "__main__":
    unittest.main()
