from time import sleep

import boto3
from prefect import task


@task
def terminate_batch_job(job_arn: str) -> None:
    client = boto3.client("batch")
    client.terminate_job(jobId=job_arn, reason="Prefect workflow termination")
    sleep(60)
