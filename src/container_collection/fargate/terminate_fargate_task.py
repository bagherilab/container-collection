import boto3


def terminate_fargate_task(cluster: str, task_arn: str) -> None:
    client = boto3.client("ecs")
    client.stop_task(cluster=cluster, task=task_arn, reason="Prefect workflow termination")
