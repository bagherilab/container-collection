from prefect import Task
import docker
from arcadeio.containers.simulate.local import JobLocalContainer, DockerContainer


class SubmitJobsLocalTask(Task):
    """Task for submitting jobs to local Docker."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, job: JobLocalContainer) -> list[DockerContainer]:
        dockers = []

        if not job.job_names:
            return dockers

        client = docker.APIClient(base_url="unix://var/run/docker.sock")
        host_config = client.create_host_config(binds=[job.directory + ":/mnt"])

        for run, job_name in zip(job.runs, job.job_names):
            # Create the container.
            container = client.create_container(**run, host_config=host_config)
            container_id = container.get("Id")
            dockers.append(DockerContainer(job_name, container_id))

            # Start the container.
            client.start(container=container_id)

        return dockers
