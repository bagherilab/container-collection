import os
import unittest
from unittest import mock

import boto3
from moto import mock_aws

from container_collection.batch.register_batch_job import register_batch_job

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
class TestRegisterBatchJob(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "job-definition-name"
        self.image = "jobimage:latest"
        self.vcpus = 1
        self.memory = 256

    @mock_aws
    def test_register_batch_job_new_definition(self):
        job_definition = {
            "jobDefinitionName": self.name,
            "type": "container",
            "containerProperties": {
                "image": self.image,
                "vcpus": self.vcpus,
                "memory": self.memory,
            },
        }

        expected_job_arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:job-definition/{self.name}:1"

        job_arn = register_batch_job(job_definition)

        batch_client = boto3.client("batch")
        response = batch_client.describe_job_definitions(jobDefinitions=[job_arn])
        registered_job = response["jobDefinitions"][0]

        self.assertEqual(expected_job_arn, job_arn)
        self.assertEqual(self.name, registered_job["jobDefinitionName"])
        self.assertEqual(self.image, registered_job["containerProperties"]["image"])
        self.assertEqual(self.vcpus, registered_job["containerProperties"]["vcpus"])
        self.assertEqual(self.memory, registered_job["containerProperties"]["memory"])

    @mock_aws
    def test_register_batch_job_updated_definition(self):
        vcpus_modified = self.vcpus + 1

        first_job_definition = {
            "jobDefinitionName": self.name,
            "type": "container",
            "containerProperties": {
                "image": self.image,
                "vcpus": self.vcpus,
                "memory": self.memory,
            },
        }

        second_job_definition = {
            "jobDefinitionName": self.name,
            "type": "container",
            "containerProperties": {
                "image": self.image,
                "vcpus": vcpus_modified,
                "memory": self.memory,
            },
        }

        expected_job_arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:job-definition/{self.name}:2"

        batch_client = boto3.client("batch")
        batch_client.register_job_definition(**first_job_definition)
        job_arn = register_batch_job(second_job_definition)

        response = batch_client.describe_job_definitions(jobDefinitions=[job_arn])
        registered_job = response["jobDefinitions"][0]

        self.assertEqual(expected_job_arn, job_arn)
        self.assertEqual(self.name, registered_job["jobDefinitionName"])
        self.assertEqual(self.image, registered_job["containerProperties"]["image"])
        self.assertEqual(vcpus_modified, registered_job["containerProperties"]["vcpus"])
        self.assertEqual(self.memory, registered_job["containerProperties"]["memory"])

    @mock_aws
    def test_register_batch_job_existing_definition(self):
        job_definition = {
            "jobDefinitionName": self.name,
            "type": "container",
            "containerProperties": {
                "image": self.image,
                "vcpus": self.vcpus,
                "memory": self.memory,
            },
        }

        expected_job_arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:job-definition/{self.name}:1"

        batch_client = boto3.client("batch")
        batch_client.register_job_definition(**job_definition)
        job_arn = register_batch_job(job_definition)

        response = batch_client.describe_job_definitions(jobDefinitions=[job_arn])
        registered_job = response["jobDefinitions"][0]

        self.assertEqual(expected_job_arn, job_arn)
        self.assertEqual(self.name, registered_job["jobDefinitionName"])
        self.assertEqual(self.image, registered_job["containerProperties"]["image"])
        self.assertEqual(self.vcpus, registered_job["containerProperties"]["vcpus"])
        self.assertEqual(self.memory, registered_job["containerProperties"]["memory"])


if __name__ == "__main__":
    unittest.main()
