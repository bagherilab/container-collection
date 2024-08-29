import sys
import unittest
from unittest import mock

import boto3

from container_collection.fargate.terminate_fargate_task import (
    TERMINATION_REASON,
    terminate_fargate_task,
)


class TestTerminateFargateTask(unittest.TestCase):
    @mock.patch.object(
        sys.modules["container_collection.fargate.terminate_fargate_task"],
        "boto3",
        return_value=mock.MagicMock(spec=boto3),
    )
    @mock.patch.object(
        sys.modules["container_collection.fargate.terminate_fargate_task"], "sleep", lambda _: None
    )
    def test_terminate_fargate_task(self, boto3_mock):
        ecs_mock = mock.MagicMock()
        boto3_mock.client.return_value = ecs_mock

        task_arn = "task-arn"
        cluster = "cluster"
        terminate_fargate_task(cluster, task_arn)

        ecs_mock.stop_task.assert_called_with(
            cluster=cluster, task=task_arn, reason=TERMINATION_REASON
        )


if __name__ == "__main__":
    unittest.main()
