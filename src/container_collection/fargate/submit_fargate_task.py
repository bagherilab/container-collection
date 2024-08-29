from typing import Any

import boto3


def submit_fargate_task(
    name: str,
    task_definition_arn: str,
    user: str,
    cluster: str,
    security_groups: list[str],
    subnets: list[str],
    command: list[str],
    **kwargs: Any,
) -> list[str]:
    """
    Submit task to AWS Fargate.

    Parameters
    ----------
    name
        Task name.
    task_definition_arn
        Task definition ARN.
    user
        User name prefix for task name.
    cluster
        ECS cluster name.
    security_groups
        List of security groups.
    subnets
        List of subnets.
    command
        Command list passed to container.
    **kwargs
        Additional parameters for task submission. The keyword arguments are
        passed to `boto3` ECS client method `run_task`.

    Returns
    -------
    :
        Task ARN.
    """

    default_task_submission = {
        "taskDefinition": task_definition_arn,
        "capacityProviderStrategy": [
            {"capacityProvider": "FARGATE", "weight": 1},
            {"capacityProvider": "FARGATE_SPOT", "weight": 1},
        ],
        "cluster": cluster,
        "platformVersion": "LATEST",
        "count": 1,
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": subnets,
                "assignPublicIp": "ENABLED",
                "securityGroups": security_groups,
            }
        },
        "overrides": {
            "containerOverrides": [
                {
                    "name": f"{user}_{name}",
                    "command": command,
                }
            ]
        },
    }

    client = boto3.client("ecs")
    task_submission = default_task_submission | kwargs
    response = client.run_task(**task_submission)

    return response["tasks"][0]["taskArn"]
