import importlib
import sys

from prefect import task

from .check_batch_job import check_batch_job
from .get_batch_logs import get_batch_logs
from .make_batch_job import make_batch_job
from .register_batch_job import register_batch_job
from .submit_batch_job import submit_batch_job
from .terminate_batch_job import terminate_batch_job

TASK_MODULES = [
    check_batch_job,
    get_batch_logs,
    make_batch_job,
    register_batch_job,
    submit_batch_job,
    terminate_batch_job,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
