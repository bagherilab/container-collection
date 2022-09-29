from arcadeio.containers.simulate import JobContainer


class JobAWSContainer(JobContainer):
    """Container for AWS simulation job."""

    def __init__(
        self,
        job_names: list[str],
        input_files: list[str],
        bucket: str,
        folder: str,
        definition: dict,
        submission: dict,
    ):
        super().__init__(job_names, input_files)
        self.bucket = bucket
        self.folder = folder
        self.definition = definition
        self.submission = submission

    def __str__(self):
        strs = [f" {'BUCKET':<10} : s3://{self.bucket}", f" {'FOLDER':<10} : {self.folder}"]
        return "\n".join(strs) + "\n" + super().__str__()
