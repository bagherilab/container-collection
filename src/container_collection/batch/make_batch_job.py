from prefect import task


@task
def make_batch_job(
    name: str,
    image: str,
    account: str,
    region: str,
    user: str,
    vcpus: int,
    memory: int,
    prefix: str,
) -> dict:
    return {
        "jobDefinitionName": f"{user}_{name}",
        "type": "container",
        "containerProperties": {
            "image": f"{account}.dkr.ecr.{region}.amazonaws.com/{user}/{image}",
            "vcpus": vcpus,
            "memory": memory,
            "environment": [
                {"name": "SIMULATION_TYPE", "value": "AWS"},
                {"name": "BATCH_WORKING_URL", "value": prefix},
                {"name": "FILE_SET_NAME", "value": name},
            ],
            "jobRoleArn": f"arn:aws:iam::{account}:role/BatchJobRole",
        },
    }
