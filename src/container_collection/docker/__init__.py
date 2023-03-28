import importlib
import sys

from prefect import task

from .check_docker_job import check_docker_job
from .clean_docker_job import clean_docker_job
from .create_docker_volume import create_docker_volume
from .get_docker_logs import get_docker_logs
from .make_docker_job import make_docker_job
from .remove_docker_volume import remove_docker_volume
from .run_docker_command import run_docker_command
from .submit_docker_job import submit_docker_job
from .terminate_docker_job import terminate_docker_job

TASK_MODULES = [
    check_docker_job,
    clean_docker_job,
    create_docker_volume,
    get_docker_logs,
    make_docker_job,
    remove_docker_volume,
    run_docker_command,
    submit_docker_job,
    terminate_docker_job,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
