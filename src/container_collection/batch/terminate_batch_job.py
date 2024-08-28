from time import sleep

import boto3

TERMINATION_REASON = "Prefect workflow termination"


def terminate_batch_job(job_arn: str) -> None:
    """
    Terminate job on AWS Batch.

    Task will sleep for 1 minute after sending the termination request.

    Parameters
    ----------
    job_arn
        Job ARN.
    """

    client = boto3.client("batch")
    client.terminate_job(jobId=job_arn, reason=TERMINATION_REASON)
    sleep(60)
