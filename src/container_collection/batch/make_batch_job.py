from __future__ import annotations


def make_batch_job(
    name: str,
    image: str,
    vcpus: int,
    memory: int,
    environment: list[dict[str, str]] | None = None,
    job_role_arn: str | None = None,
) -> dict:
    """
    Create batch job definition.

    Docker images on the Docker Hub registry are available by default, and can
    be specified using ``image:tag``. Otherwise, use ``repository/image:tag``.

    Environment variables are passed as key-value pairs using the following
    structure:

    .. code-block:: python

        [
            { "name" : "envName1", "value" : "envValue1" },
            { "name" : "envName2", "value" : "envValue2" },
            ...
        ]

    Parameters
    ----------
    name
        Job definition name.
    image
        Docker image.
    vcpus
        Number of vCPUs to reserve for the container.
    memory
        Memory limit available to the container
    environment
        List of environment variables as key-value pairs.
    job_role_arn
        ARN for IAM role for the job container.

    Returns
    -------
    :
        Job definition.
    """

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
