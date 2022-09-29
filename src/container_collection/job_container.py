class JobContainer:
    """Container for general simulation job."""

    def __init__(self, job_names: list[str], input_files: list[str]):
        self.job_names = job_names
        self.input_files = input_files

    def __str__(self):
        from tabulate import tabulate

        headers = ["JOB", "INPUT"]
        table = []
        for job_name, input_file in zip(self.job_names, self.input_files):
            table.append([job_name, input_file])
        table.sort()

        return tabulate(table, headers=headers, tablefmt="pretty")
