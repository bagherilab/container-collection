from prefect import Task
from prefect.utilities.aws import get_boto_client
from arcadeio.containers.simulate.aws import JobAWSContainer, BatchContainer


class SubmitJobsAWSTask(Task):
    """Task for submitting jobs to AWS Batch."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, job: JobAWSContainer) -> list[BatchContainer]:
        batches = []

        if not job.job_names:
            return batches

        batch_client = get_boto_client("batch")
        response = batch_client.register_job_definition(**job.definition)
        def_arn = response["jobDefinitionArn"]

        response = batch_client.submit_job(**job.submission)
        job_arn = response["jobArn"]
        job_id = response["jobId"]

        if "arrayProperties" in job.submission:
            for i, job_name in enumerate(job.job_names):
                batches.append(BatchContainer(job_name, job_arn, job_id + ":" + str(i), def_arn))
        else:
            batches.append(BatchContainer(job.job_names[0], job_arn, job_id, def_arn))

        return batches
