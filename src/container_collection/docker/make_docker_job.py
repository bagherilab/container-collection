from typing import Optional


def make_docker_job(name: str, image: str, environment: Optional[list[str]] = None) -> dict:
    """
    Create docker job definition.

    Environment variables are passed as strings using the following structure:

    .. code-block:: python

        [
            "envName1=envValue1",
            "envName2=envValue2",
            ...
        ]

    Parameters
    ----------
    name
        Job definition name.
    image
        Docker image.
    environment
        List of environment variables as strings.

    Returns
    -------
    :
        Job definition.
    """

    job_definition = {
        "image": image,
        "name": name,
        "volumes": ["/mnt"],
    }

    if environment is not None:
        job_definition["environment"] = environment

    return job_definition
