import json
import os
import unittest
from unittest import mock

import boto3
from moto import mock_aws

from container_collection.batch.submit_batch_job import submit_batch_job

ACCOUNT = "123123123123"
REGION = "default-region"
QUEUE = "queue-name"
COMPUTE = "compute-environment-name"


@mock.patch.dict(
    os.environ,
    {
        "MOTO_ALLOW_NONEXISTENT_REGION": "True",
        "MOTO_ACCOUNT_ID": ACCOUNT,
        "AWS_DEFAULT_REGION": REGION,
    },
)
class TestSubmitBatchJob(unittest.TestCase):
    def setUp(self) -> None:
        self.job_definition = {
            "jobDefinitionName": "job-definition-name",
            "type": "container",
            "containerProperties": {
                "image": "jobimage:latest",
                "vcpus": 1,
                "memory": 256,
            },
        }

        arn_prefix = f"arn:aws:batch:{REGION}:{ACCOUNT}"
        self.job_definition_arn = f"{arn_prefix}:job-definition/job-definition-name:1"
        self.queue_arn = f"{arn_prefix}:job-queue/{QUEUE}"

    @mock_aws(config={"batch": {"use_docker": False}})
    def test_submit_batch_job_single_job(self):
        name = "job-name"
        user = "user"
        size = 1

        initialize_infrastructure()
        batch_client = boto3.client("batch")
        batch_client.register_job_definition(**self.job_definition)

        job_arns = submit_batch_job(name, self.job_definition_arn, user, QUEUE, size)

        jobs = batch_client.describe_jobs(jobs=job_arns)
        self.assertEqual(size, len(jobs["jobs"]))
        self.assertEqual(f"{user}_{name}", jobs["jobs"][0]["jobName"])
        self.assertEqual(job_arns[0], jobs["jobs"][0]["jobArn"])
        self.assertEqual(self.queue_arn, jobs["jobs"][0]["jobQueue"])
        self.assertEqual(self.job_definition_arn, jobs["jobs"][0]["jobDefinition"])

    @mock_aws(config={"batch": {"use_docker": False}})
    def test_submit_batch_job_multiple_jobs(self):
        name = "job-name"
        user = "user"
        size = 3

        initialize_infrastructure()
        batch_client = boto3.client("batch")
        batch_client.register_job_definition(**self.job_definition)

        job_arns = submit_batch_job(name, self.job_definition_arn, user, QUEUE, size)

        jobs = batch_client.describe_jobs(jobs=job_arns)
        self.assertEqual(size, len(jobs["jobs"]))

        for index in range(size):
            self.assertEqual(f"{user}_{name}", jobs["jobs"][index]["jobName"])
            self.assertEqual(job_arns[index], jobs["jobs"][index]["jobArn"])
            self.assertEqual(self.queue_arn, jobs["jobs"][index]["jobQueue"])
            self.assertEqual(self.job_definition_arn, jobs["jobs"][index]["jobDefinition"])


def initialize_infrastructure() -> None:
    # Create clients.
    ec2_client = boto3.client("ec2")
    iam_client = boto3.client("iam")
    batch_client = boto3.client("batch")

    # Create VPC.
    vpc = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    # Create subnet.
    subnet = ec2_client.create_subnet(
        AvailabilityZone="us-east-1a", CidrBlock="10.0.0.0/18", VpcId=vpc_id
    )
    subnet_id = subnet["Subnet"]["SubnetId"]

    # Create security group.
    security_group = ec2_client.create_security_group(
        Description="test security group description",
        GroupName="security-group-name",
        VpcId=vpc_id,
    )
    security_group_id = security_group["GroupId"]

    # Create service role.
    service_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "batch.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    iam_client.create_role(
        RoleName="batch-service-role", AssumeRolePolicyDocument=service_role_policy
    )

    # Create instance role.
    iam_client.create_instance_profile(InstanceProfileName="instance-role")
    iam_client.add_role_to_instance_profile(
        InstanceProfileName="instance-role", RoleName="batch-service-role"
    )

    # Create compute environment.
    batch_client.create_compute_environment(
        computeEnvironmentName=COMPUTE,
        type="MANAGED",
        serviceRole=f"arn:aws:iam::{ACCOUNT}:role/batch-service-role",
        computeResources={
            "type": "EC2",
            "minvCpus": 1,
            "maxvCpus": 2,
            "subnets": [subnet_id],
            "securityGroupIds": [security_group_id],
            "instanceRole": f"arn:aws:iam::{ACCOUNT}:instance-profile/instance-role",
            "instanceTypes": ["t2.micro"],
        },
    )

    # Create job queue.
    arn_prefix = f"arn:aws:batch:{REGION}:{ACCOUNT}"
    batch_client.create_job_queue(
        jobQueueName=QUEUE,
        state="ENABLED",
        priority=1,
        computeEnvironmentOrder=[
            {
                "order": 1,
                "computeEnvironment": f"{arn_prefix}:compute-environment/{COMPUTE}",
            },
        ],
    )


if __name__ == "__main__":
    unittest.main()
