import boto3
from prefect import task


@task
def terminate_fargate_task(cluster: str, task_arn: str) -> None:
    client = boto3.client("ecs")
    client.stop_task(cluster=cluster, task=task_arn, reason="Prefect workflow termination")
