import os
import time
import unittest
from unittest import mock

import boto3
import botocore
from moto import mock_aws

from container_collection.batch.get_batch_logs import LOG_GROUP_NAME, get_batch_logs

ACCOUNT = "123123123123"
REGION = "default-region"

original_make_api_call = botocore.client.BaseClient._make_api_call


def mock_make_api_call(self, operation_name, kwarg):
    if operation_name == "DescribeJobs":
        return {
            "jobs": [
                {
                    "jobArn": kwarg["jobs"][0],
                    "container": {
                        "logStreamName": f'{kwarg["jobs"][0]}-log-stream-name',
                    },
                }
            ]
        }
    return original_make_api_call(self, operation_name, kwarg)


@mock.patch.dict(
    os.environ,
    {
        "MOTO_ALLOW_NONEXISTENT_REGION": "True",
        "MOTO_ACCOUNT_ID": ACCOUNT,
        "AWS_DEFAULT_REGION": REGION,
    },
)
class TestGetBatchLogs(unittest.TestCase):
    def setUp(self) -> None:
        self.job_arn = "job-arn"
        self.log_stream_name = f"{self.job_arn}-log-stream-name"

    @mock_aws
    def test_get_batch_logs_no_continuation_no_filter(self):
        log_filter = ""
        timestamp = round(time.time() * 1000)
        messages = [f"Event {i}" for i in range(10)]
        expected_logs = "\n".join(messages)

        client = boto3.client("logs")
        client.create_log_group(logGroupName=LOG_GROUP_NAME)
        client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=self.log_stream_name)
        client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=self.log_stream_name,
            logEvents=[{"timestamp": timestamp, "message": message} for message in messages],
        )

        with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            logs = get_batch_logs(self.job_arn, log_filter)
            self.assertEqual(expected_logs, logs)

    @mock_aws
    def test_get_batch_logs_no_continuation_with_filter(self):
        log_filter = "INCLUDE"
        timestamp = round(time.time() * 1000)
        include_messages = [f"Event INCLUDE {i}" for i in range(10)]
        exclude_messages = [f"Event EXCLUDE {i}" for i in range(10)]
        messages = [val for pair in zip(include_messages, exclude_messages) for val in pair]
        expected_logs = "\n".join(include_messages)

        client = boto3.client("logs")
        client.create_log_group(logGroupName=LOG_GROUP_NAME)
        client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=self.log_stream_name)
        client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=self.log_stream_name,
            logEvents=[{"timestamp": timestamp, "message": message} for message in messages],
        )

        with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            logs = get_batch_logs(self.job_arn, log_filter)
            self.assertEqual(expected_logs, logs)

    @mock_aws
    def test_get_batch_logs_with_continuation_no_filter(self):
        log_filter = ""
        timestamp = round(time.time() * 1000)
        messages = [f"Event {i}" for i in range(10001)]
        expected_logs = "\n".join(messages)

        client = boto3.client("logs")
        client.create_log_group(logGroupName=LOG_GROUP_NAME)
        client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=self.log_stream_name)
        client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=self.log_stream_name,
            logEvents=[{"timestamp": timestamp, "message": message} for message in messages],
        )

        with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            logs = get_batch_logs(self.job_arn, log_filter)
            self.assertEqual(expected_logs, logs)

    @mock_aws
    def test_get_batch_logs_with_continuation_with_filter(self):
        log_filter = "INCLUDE"
        timestamp = round(time.time() * 1000)
        include_messages = [f"Event INCLUDE {i}" for i in range(10001)]
        exclude_messages = [f"Event EXCLUDE {i}" for i in range(10001)]
        messages = [val for pair in zip(include_messages, exclude_messages) for val in pair]
        expected_logs = "\n".join(include_messages)

        client = boto3.client("logs")
        client.create_log_group(logGroupName=LOG_GROUP_NAME)
        client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=self.log_stream_name)
        client.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=self.log_stream_name,
            logEvents=[{"timestamp": timestamp, "message": message} for message in messages],
        )

        with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            logs = get_batch_logs(self.job_arn, log_filter)
            self.assertEqual(expected_logs, logs)


if __name__ == "__main__":
    unittest.main()
