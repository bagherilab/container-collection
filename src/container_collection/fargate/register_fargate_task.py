import boto3
from prefect import task
from deepdiff import DeepDiff


@task
def register_fargate_task(task_definition: dict) -> None:
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
