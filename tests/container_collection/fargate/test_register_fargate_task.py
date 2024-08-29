import os
import unittest
from unittest import mock

import boto3
from moto import mock_aws

from container_collection.fargate.register_fargate_task import register_fargate_task

ACCOUNT = "123123123123"
REGION = "default-region"


@mock.patch.dict(
    os.environ,
    {
        "MOTO_ALLOW_NONEXISTENT_REGION": "True",
        "MOTO_ACCOUNT_ID": ACCOUNT,
        "AWS_DEFAULT_REGION": REGION,
    },
)
class TestRegisterFargateTask(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "task-definition-name"
        self.image = "jobimage:latest"
        self.vcpus = "1"
        self.memory = "256"

    @mock_aws
    def test_register_fargate_task_new_definition(self):
        task_definition = {
            "family": self.name,
            "containerDefinitions": [
                {
                    "name": self.name,
                    "image": self.image,
                }
            ],
            "cpu": self.vcpus,
            "memory": self.memory,
        }

        expected_task_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{self.name}:1"

        task_arn = register_fargate_task(task_definition)

        ecs_client = boto3.client("ecs")
        response = ecs_client.describe_task_definition(taskDefinition=self.name)
        registered_task = response["taskDefinition"]

        self.assertEqual(expected_task_arn, task_arn)
        self.assertEqual(self.name, registered_task["family"])
        self.assertEqual(self.image, registered_task["containerDefinitions"][0]["image"])
        self.assertEqual(self.vcpus, registered_task["cpu"])
        self.assertEqual(self.memory, registered_task["memory"])

    @mock_aws
    def test_register_fargate_task_updated_definition(self):
        vcpus_modified = str(int(self.vcpus) + 1)

        first_task_definition = {
            "family": self.name,
            "containerDefinitions": [
                {
                    "name": self.name,
                    "image": self.image,
                }
            ],
            "cpu": self.vcpus,
            "memory": self.memory,
        }

        second_task_definition = {
            "family": self.name,
            "containerDefinitions": [
                {
                    "name": self.name,
                    "image": self.image,
                }
            ],
            "cpu": vcpus_modified,
            "memory": self.memory,
        }

        expected_task_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{self.name}:2"

        ecs_client = boto3.client("ecs")
        ecs_client.register_task_definition(**first_task_definition)
        task_arn = register_fargate_task(second_task_definition)

        response = ecs_client.describe_task_definition(taskDefinition=self.name)
        registered_task = response["taskDefinition"]

        self.assertEqual(expected_task_arn, task_arn)
        self.assertEqual(self.name, registered_task["family"])
        self.assertEqual(self.image, registered_task["containerDefinitions"][0]["image"])
        self.assertEqual(vcpus_modified, registered_task["cpu"])
        self.assertEqual(self.memory, registered_task["memory"])

    @mock_aws
    def test_register_fargate_task_existing_definition(self):
        task_definition = {
            "family": self.name,
            "containerDefinitions": [
                {
                    "name": self.name,
                    "image": self.image,
                }
            ],
            "cpu": self.vcpus,
            "memory": self.memory,
        }

        expected_task_arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{self.name}:1"

        ecs_client = boto3.client("ecs")
        ecs_client.register_task_definition(**task_definition)
        task_arn = register_fargate_task(task_definition)

        response = ecs_client.describe_task_definition(taskDefinition=self.name)
        registered_task = response["taskDefinition"]

        self.assertEqual(expected_task_arn, task_arn)
        self.assertEqual(self.name, registered_task["family"])
        self.assertEqual(self.image, registered_task["containerDefinitions"][0]["image"])
        self.assertEqual(self.vcpus, registered_task["cpu"])
        self.assertEqual(self.memory, registered_task["memory"])


if __name__ == "__main__":
    unittest.main()
