from prefect import Task


class MakeJobsTask(Task):
    """General methods for tasks that making jobs."""

    @staticmethod
    def _make_file_name(name, extension, i):
        return name + "_" + str(i) + extension

    @staticmethod
    def _make_job_names(name, jobs):
        return [f"{name} [ {i:>3} ]" for i in range(jobs)]
