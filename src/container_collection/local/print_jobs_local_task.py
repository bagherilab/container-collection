from prefect import Task
from arcadeio.containers.simulate.local import JobLocalContainer, DockerContainer


class PrintJobsLocalTask(Task):
    """Task for printing information on local jobs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, local: JobLocalContainer, dockers: list[DockerContainer]):
        print()
        print(local)
        print()

        for docker in dockers:
            print(docker)

        print()
