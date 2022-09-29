import os
import prefect
from arcadeio.tasks.simulate import MakeJobsTask
from arcadeio.containers import TemplateContainer
from arcadeio.containers.simulate import InputContainer
from arcadeio.containers.simulate.local import JobLocalContainer


class MakeJobsLocalTask(MakeJobsTask):
    """Task for making jobs that run on local Docker."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, inputs: InputContainer, template: TemplateContainer) -> JobLocalContainer:
        # Context variables.
        flow_run_name = prefect.context.flow_run_name
        timestamp = prefect.context.today
        directory = prefect.context.working_directory

        # Save input files to working directory.
        full_directory = self._make_working_directories(directory, flow_run_name, timestamp)
        input_files = self._save_input_files(
            full_directory, template.name, inputs.contents, inputs.header, inputs.footer
        )

        # Create jobs.
        num_inputs = len(input_files)
        job_names = MakeJobsTask._make_job_names(template.name, num_inputs)
        runs = self._create_job_runs(template.name, num_inputs, prefect.context.jar_image)

        return JobLocalContainer(job_names, input_files, full_directory, runs)

    @staticmethod
    def _make_working_directories(directory, name, timestamp):
        full_directory = directory + name + "/" + timestamp + "/"
        os.makedirs(full_directory, exist_ok=True)

        folders = ["inputs/", "outputs/"]
        for folder in folders:
            folder_directory = full_directory + folder
            os.makedirs(folder_directory, exist_ok=True)

        return full_directory

    @staticmethod
    def _save_input_files(directory, name, contents, header, footer):
        contents = [c for c in contents if len(c) > 0]
        input_files = []

        for i, content in enumerate(contents):
            filename = MakeJobsTask._make_file_name(name, ".xml", i)
            filepath = directory + "inputs/" + filename
            input_files.append(filename)

            with open(filepath, "w") as file:
                file.write(header + "\n".join(content) + footer)

        return input_files

    @staticmethod
    def _create_job_runs(name, size, image):
        job_runs = []

        for i in range(size):
            job_run = {
                "image": image,
                "name": f"{name}-run-{i}",
                "environment": [
                    "SIMULATION_TYPE=LOCAL",
                    f"FILE_SET_NAME={name}",
                    f"JOB_ARRAY_INDEX={i}",
                ],
                "volumes": ["/mnt"],
            }

            job_runs.append(job_run)

        return job_runs
