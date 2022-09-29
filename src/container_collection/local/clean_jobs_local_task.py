import docker
import prefect
from prefect import Task
from arcadeio.containers.simulate.local import DockerContainer


class CleanJobsLocalTask(Task):
    """Task for cleaning local Docker workspace."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = docker.APIClient(base_url="unix://var/run/docker.sock")

    def run(self, dockers: list[DockerContainer]):
        if prefect.context.flags["clean"] is False:
            return

        self._remove_exited_containers(dockers)

    def _remove_exited_containers(self, dockers):
        for doc in dockers:
            status = self.client.containers(all=True, filters={"id": doc.container})[0]["State"]

            if status != "running":
                self.client.remove_container(container=doc.container)
