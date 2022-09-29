from prefect import Task
from arcadeio.containers.simulate.aws import JobAWSContainer, BatchContainer


class PrintJobsAWSTask(Task):
    """Task for printing information on AWS jobs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, aws: JobAWSContainer, batches: list[BatchContainer]):
        print()
        print(aws)
        print()

        for batch in batches:
            print(batch)

        print()
