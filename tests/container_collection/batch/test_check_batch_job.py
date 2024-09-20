from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

import boto3
from prefect import flow
from prefect.exceptions import FailedRun
from prefect.testing.utilities import prefect_test_harness

from container_collection.batch import check_batch_job as check_batch_job_task
from container_collection.batch.check_batch_job import RETRIES_EXCEEDED_EXIT_CODE, check_batch_job

SUCCEEDED_EXIT_CODE = 0
FAILED_EXIT_CODE = 1


def make_describe_jobs_response(status: str | None, exit_code: int | None):
    if status is None:
        return {"jobs": []}
    if status in ("SUCCEEDED", "FAILED"):
        return {"jobs": [{"status": status, "attempts": [{"container": {"exitCode": exit_code}}]}]}
    return {"jobs": [{"status": status}]}


def make_boto_mock(statuses: list[str | None], exit_code: int | None = None):
    batch_mock = mock.MagicMock()
    boto3_mock = mock.MagicMock(spec=boto3)
    boto3_mock.client.return_value = batch_mock
    batch_mock.describe_jobs.side_effect = [
        make_describe_jobs_response(status, exit_code) for status in statuses
    ]
    return boto3_mock


@flow
def run_task_under_flow(max_retries: int):
    return check_batch_job_task("job-arn", max_retries)


@mock.patch.dict(
    os.environ,
    {"PREFECT_LOGGING_LEVEL": "CRITICAL"},
)
class TestCheckBatchJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with prefect_test_harness():
            yield

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["RUNNING"]),
    )
    def test_check_batch_job_as_method_running_throws_exception(self):
        with self.assertRaises(RuntimeError):
            check_batch_job("job-arn", 0)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock([None, "PENDING", "RUNNING"]),
    )
    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"], "sleep", mock.Mock()
    )
    def test_check_batch_job_as_method_running_with_waits_throws_exception(self):
        with self.assertRaises(RuntimeError):
            check_batch_job("job-arn", 0)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["SUCCEEDED"], SUCCEEDED_EXIT_CODE),
    )
    def test_check_batch_job_as_method_succeeded(self):
        exit_code = check_batch_job("job-arn", 0)
        self.assertEqual(SUCCEEDED_EXIT_CODE, exit_code)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["FAILED"], FAILED_EXIT_CODE),
    )
    def test_check_batch_job_as_method_failed(self):
        exit_code = check_batch_job("job-arn", 0)
        self.assertEqual(FAILED_EXIT_CODE, exit_code)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["RUNNING"]),
    )
    def test_check_batch_job_as_task_running_below_max_retries_throws_failed_run(self):
        max_retries = 1
        with self.assertRaises(FailedRun):
            run_task_under_flow(max_retries)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock([None, "PENDING", "RUNNING"]),
    )
    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"], "sleep", mock.Mock()
    )
    def test_check_batch_job_as_task_running_below_max_retries_with_waits_throws_failed_run(self):
        max_retries = 1
        with self.assertRaises(FailedRun):
            run_task_under_flow(max_retries)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock([None]),
    )
    def test_check_batch_job_as_task_max_retries_exceeded(self):
        max_retries = 0
        exit_code = run_task_under_flow(max_retries)
        self.assertEqual(RETRIES_EXCEEDED_EXIT_CODE, exit_code)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["SUCCEEDED"], SUCCEEDED_EXIT_CODE),
    )
    def test_check_batch_job_as_task_succeeded(self):
        max_retries = 1
        exit_code = run_task_under_flow(max_retries)
        self.assertEqual(SUCCEEDED_EXIT_CODE, exit_code)

    @mock.patch.object(
        sys.modules["container_collection.batch.check_batch_job"],
        "boto3",
        make_boto_mock(["FAILED"], FAILED_EXIT_CODE),
    )
    def test_check_batch_job_as_task_failed(self):
        max_retries = 1
        exit_code = run_task_under_flow(max_retries)
        self.assertEqual(FAILED_EXIT_CODE, exit_code)


if __name__ == "__main__":
    unittest.main()
