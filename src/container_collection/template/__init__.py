import importlib
import sys

from prefect import task

from .generate_input_contents import generate_input_contents

TASK_MODULES = [
    generate_input_contents,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
