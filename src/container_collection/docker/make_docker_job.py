from prefect import task


@task
def make_docker_job(name: str, image: str, index: int) -> dict:
    return {
        "image": image,
        "name": f"{name}_{index}",
        "environment": [
            "SIMULATION_TYPE=LOCAL",
            f"FILE_SET_NAME={name}",
            f"JOB_ARRAY_INDEX={index}",
        ],
        "volumes": ["/mnt"],
    }
