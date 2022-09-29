import prefect
from prefect import Task
from prefect.utilities.aws import get_boto_client
from arcadeio.containers.simulate.aws import BatchContainer


class CleanJobsAWSTask(Task):
    """Task for cleaning AWS Batch workspace."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = get_boto_client("batch")

    def run(self, batches: list[BatchContainer]):
        if prefect.context.flags["clean"] is False:
            return

        self._deregister_job_definitions(batches)

    def _deregister_job_definitions(self, batches):
        definitions = {bat.definition_arn for bat in batches}
        for definition in definitions:
            self.client.deregister_job_definition(jobDefinition=definition)
