import sys
import unittest
from unittest import mock

import boto3

from container_collection.batch.terminate_batch_job import TERMINATION_REASON, terminate_batch_job


class TestTerminateBatchJob(unittest.TestCase):
    @mock.patch.object(
        sys.modules["container_collection.batch.terminate_batch_job"],
        "boto3",
        return_value=mock.MagicMock(spec=boto3),
    )
    @mock.patch.object(
        sys.modules["container_collection.batch.terminate_batch_job"], "sleep", lambda _: None
    )
    def test_terminate_batch_job(self, boto3_mock):
        batch_mock = mock.MagicMock()
        boto3_mock.client.return_value = batch_mock

        job_arn = "job-arn"
        terminate_batch_job(job_arn)

        batch_mock.terminate_job.assert_called_with(jobId=job_arn, reason=TERMINATION_REASON)


if __name__ == "__main__":
    unittest.main()
