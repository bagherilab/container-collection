import boto3
from deepdiff import DeepDiff
from prefect import task


@task
def register_batch_job(job_definition: dict) -> None:
    client = boto3.client("batch")

    response = client.describe_job_definitions(
        jobDefinitionName=job_definition["jobDefinitionName"],
    )

    if len(response["jobDefinitions"]) > 0:
        existing_definition = sorted(response["jobDefinitions"], key=lambda d: d["revision"]).pop()
        diff = DeepDiff(job_definition, existing_definition, ignore_order=True)

        if "values_changed" not in diff:
            return existing_definition["jobDefinitionArn"]

    response = client.register_job_definition(**job_definition)
    return response["jobDefinitionArn"]
