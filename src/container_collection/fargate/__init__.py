import importlib
import sys

from prefect import task

from .check_fargate_task import check_fargate_task
from .make_fargate_task import make_fargate_task
from .register_fargate_task import register_fargate_task
from .submit_fargate_task import submit_fargate_task
from .terminate_fargate_task import terminate_fargate_task

TASK_MODULES = [
    check_fargate_task,
    make_fargate_task,
    register_fargate_task,
    submit_fargate_task,
    terminate_fargate_task,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
