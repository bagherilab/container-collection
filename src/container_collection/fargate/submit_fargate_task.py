import boto3
from prefect import task
from prefect.blocks.system import Secret


@task
def submit_fargate_task(
    name: str, task_definition_arn: str, user: str, cluster: str, command: list[str]
) -> list[str]:
    security_groups = Secret.load("aws-security-groups").get().split(":")
    subnets = Secret.load("aws-subnets").get().split(":")

    task_submission = {
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
        "overrides": {"containerOverrides": [{"name": f"{user}_{name}", "command": command}]},
    }

    client = boto3.client("ecs")
    response = client.run_task(**task_submission)

    return response["tasks"][0]["taskArn"]
