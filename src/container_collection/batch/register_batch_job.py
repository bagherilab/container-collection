import boto3
from deepdiff import DeepDiff


def register_batch_job(job_definition: dict) -> str:
    """
    Register job definition to AWS Batch.

    If a definition for the given job definition name already exists, and the
    contents of the definition are not changed, then the method will return the
    existing job definition ARN rather than creating a new revision.

    Parameters
    ----------
    job_definition
        Batch job definition.

    Returns
    -------
    :
        Job definition ARN.
    """

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
