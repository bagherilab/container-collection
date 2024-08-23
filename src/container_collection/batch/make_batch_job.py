from typing import Optional


def make_batch_job(
    name: str,
    image: str,
    vcpus: int,
    memory: int,
    environment: Optional[list[dict[str, str]]] = None,
    job_role_arn: Optional[str] = None,
) -> dict:
    container_properties = {
        "image": image,
        "vcpus": vcpus,
        "memory": memory,
    }

    if environment is not None:
        container_properties["environment"] = environment

    if job_role_arn is not None:
        container_properties["jobRoleArn"] = job_role_arn

    return {
        "jobDefinitionName": name,
        "type": "container",
        "containerProperties": container_properties,
    }
