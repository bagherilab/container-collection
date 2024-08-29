import boto3
from deepdiff import DeepDiff


def register_fargate_task(task_definition: dict) -> str:
    """
    Register task definition to ECS Fargate.

    If a definition for the given task definition name already exists, and the
    contents of the definition are not changed, then the method will return the
    existing task definition ARN rather than creating a new revision.

    Parameters
    ----------
    task_definition
        Fargate task definition.

    Returns
    -------
    :
        Task definition ARN.
    """

    client = boto3.client("ecs")
    response = client.list_task_definitions(familyPrefix=task_definition["family"])

    if len(response["taskDefinitionArns"]) > 0:
        response = client.describe_task_definition(taskDefinition=task_definition["family"])
        existing_definition = response["taskDefinition"]
        diff = DeepDiff(task_definition, existing_definition, ignore_order=True)

        if "values_changed" not in diff:
            return existing_definition["taskDefinitionArn"]

    response = client.register_task_definition(**task_definition)
    return response["taskDefinition"]["taskDefinitionArn"]
