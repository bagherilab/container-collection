from time import sleep

import boto3

TERMINATION_REASON = "Termination requested by workflow."
"""Reason sent for terminating jobs from a workflow."""


def terminate_fargate_task(cluster: str, task_arn: str) -> None:
    """
    Terminate task on AWS Fargate.

    Parameters
    ----------
    cluster
        ECS cluster name.
    task_arn
        Task ARN.
    """

    client = boto3.client("ecs")
    client.stop_task(cluster=cluster, task=task_arn, reason=TERMINATION_REASON)
    sleep(60)
