class BatchContainer:
    """Container for job running on local Docker."""

    def __init__(self, name: str, job_arn: str, job_id: str, definition_arn: str):
        self.name = name
        self.job_arn = job_arn
        self.job_id = job_id
        self.definition_arn = definition_arn

    def __str__(self):
        strs = [
            f" {'NAME':<10} : {self.name}",
            f" {'ARN':<10} : {self.job_arn}",
            f" {'ID':<10} : {self.job_id}",
            f" {'DEFINITION':<10} : {self.definition_arn}",
        ]

        return "\n".join(strs) + "\n"
