from arcadeio.containers.simulate import JobContainer


class JobLocalContainer(JobContainer):
    """Container for local simulation job."""

    def __init__(
        self, job_names: list[str], input_files: list[str], directory: str, runs: list[dict]
    ):
        super().__init__(job_names, input_files)
        self.directory = directory
        self.runs = runs

    def __str__(self):
        strs = [f" {'DIRECTORY':<10} : {self.directory}"]
        return "\n".join(strs) + "\n" + super().__str__()
