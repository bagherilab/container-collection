import os
import unittest
from typing import Optional
from unittest import mock

from docker import APIClient
from prefect import flow
from prefect.exceptions import FailedRun
from prefect.testing.utilities import prefect_test_harness

from container_collection.docker import check_docker_job as check_docker_job_task
from container_collection.docker.check_docker_job import (
    RETRIES_EXCEEDED_EXIT_CODE,
    check_docker_job,
)

SUCCEEDED_EXIT_CODE = 0
FAILED_EXIT_CODE = 1


def make_client_mock(status: str, exit_code: Optional[int] = None):
    client = mock.MagicMock(spec=APIClient)
    client.containers.return_value = [{"State": status}]
    client.wait.return_value = {"StatusCode": exit_code}
    return client


@flow
def run_task_under_flow(client: APIClient, max_retries: int):
    return check_docker_job_task(client, "container-id", max_retries)


@mock.patch.dict(
    os.environ,
    {"PREFECT_LOGGING_LEVEL": "CRITICAL"},
)
class TestCheckDockerJob(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with prefect_test_harness():
            yield

    def test_check_docker_job_as_method_running_throws_exception(self):
        client = make_client_mock("running")
        with self.assertRaises(RuntimeError):
            check_docker_job(client, "container-id", 0)
            client.containers.assert_called_with(all=True, filters={"id": "container-id"})

    def test_check_docker_job_as_method_succeeded(self):
        client = make_client_mock("exited", SUCCEEDED_EXIT_CODE)
        exit_code = check_docker_job(client, "container-id", 0)
        self.assertEqual(SUCCEEDED_EXIT_CODE, exit_code)
        client.containers.assert_called_with(all=True, filters={"id": "container-id"})

    def test_check_docker_job_as_method_failed(self):
        client = make_client_mock("exited", FAILED_EXIT_CODE)
        exit_code = check_docker_job(client, "container-id", 0)
        self.assertEqual(FAILED_EXIT_CODE, exit_code)
        client.containers.assert_called_with(all=True, filters={"id": "container-id"})

    def test_check_docker_job_as_task_running_below_max_retries_throws_failed_run(self):
        client = make_client_mock("running")
        max_retries = 1
        with self.assertRaises(FailedRun):
            run_task_under_flow(client, max_retries)
            client.containers.assert_called_with(all=True, filters={"id": "container-id"})

    def test_check_docker_job_as_task_max_retries_exceeded(self):
        client = make_client_mock("")
        max_retries = 0
        exit_code = run_task_under_flow(client, max_retries)
        self.assertEqual(RETRIES_EXCEEDED_EXIT_CODE, exit_code)

    def test_check_docker_job_as_task_succeeded(self):
        client = make_client_mock("exited", SUCCEEDED_EXIT_CODE)
        max_retries = 1
        exit_code = run_task_under_flow(client, max_retries)
        self.assertEqual(SUCCEEDED_EXIT_CODE, exit_code)
        client.containers.assert_called_with(all=True, filters={"id": "container-id"})

    def test_check_docker_job_as_task_failed(self):
        client = make_client_mock("exited", FAILED_EXIT_CODE)
        max_retries = 1
        exit_code = run_task_under_flow(client, max_retries)
        self.assertEqual(FAILED_EXIT_CODE, exit_code)
        client.containers.assert_called_with(all=True, filters={"id": "container-id"})


if __name__ == "__main__":
    unittest.main()
