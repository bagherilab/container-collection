import io
import prefect
from prefect.utilities.aws import get_boto_client
from arcadeio.tasks.simulate import MakeJobsTask
from arcadeio.containers import TemplateContainer
from arcadeio.containers.simulate import InputContainer
from arcadeio.containers.simulate.aws import JobAWSContainer


class MakeJobsAWSTask(MakeJobsTask):
    """Task for making jobs that run on AWS Batch."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, inputs: InputContainer, template: TemplateContainer) -> JobAWSContainer:
        # Context variables.
        flow_run_name = prefect.context.flow_run_name
        timestamp = prefect.context.today
        bucket = prefect.context.working_bucket
        user = prefect.context.user_prefix

        # Save input files to working bucket.
        full_folder = self._parse_bucket_folder(flow_run_name, timestamp)
        input_files = self._save_input_files(
            bucket,
            full_folder,
            template.name,
            inputs.contents,
            inputs.header,
            inputs.footer,
        )

        # Create jobs.
        num_inputs = len(input_files)
        job_names = MakeJobsTask._make_job_names(template.name, num_inputs)
        definition = self._create_job_definition(
            bucket,
            user,
            full_folder,
            template.name,
            prefect.context.jar_image,
            prefect.context.aws_account,
            prefect.context.aws_region,
            prefect.context.job_vcpus,
            prefect.context.job_memory,
        )
        submission = self._create_job_submission(
            definition, user, num_inputs, template.name, prefect.context.job_queue
        )

        return JobAWSContainer(job_names, input_files, bucket, full_folder, definition, submission)

    @staticmethod
    def _parse_bucket_folder(name, timestamp):
        full_folder = name + "/" + timestamp + "/"
        return full_folder

    @staticmethod
    def _save_input_files(bucket, folder, name, contents, header, footer):
        s3_client = get_boto_client("s3")

        contents = [c for c in contents if len(c) > 0]
        input_files = []

        for i, content in enumerate(contents):
            filename = MakeJobsTask._make_file_name(name, ".xml", i)
            filekey = folder + "inputs/" + filename
            input_files.append(filename)

            file_contents = header + "\n".join(content) + footer
            body = io.BytesIO(file_contents.encode())
            s3_client.put_object(Bucket=bucket, Key=filekey, Body=body)

        return input_files

    @staticmethod
    def _create_job_definition(bucket, user, folder, name, image, account, region, vcpus, memory):
        job_definition = {
            "jobDefinitionName": f"{user}_definition_{name}",
            "type": "container",
            "containerProperties": {
                "image": f"{account}.dkr.ecr.{region}.amazonaws.com/{image}",
                "vcpus": vcpus,
                "memory": memory,
                "environment": [
                    {"name": "SIMULATION_TYPE", "value": "AWS"},
                    {"name": "BATCH_WORKING_URL", "value": "s3://" + bucket + "/" + folder},
                    {"name": "FILE_SET_NAME", "value": name},
                ],
                "jobRoleArn": f"arn:aws:iam::{account}:role/BatchJobRole",
            },
        }

        return job_definition

    @staticmethod
    def _create_job_submission(job_definition, user, size, name, queue):
        job_submission = {
            "jobName": f"{user}_job_{name}",
            "jobQueue": queue,
            "jobDefinition": job_definition["jobDefinitionName"],
        }

        if size > 1:
            job_submission["arrayProperties"] = {"size": size}

        return job_submission
