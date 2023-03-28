import importlib
import sys

from prefect import task

from .filter_manifest_files import filter_manifest_files
from .find_missing_conditions import find_missing_conditions
from .summarize_manifest_files import summarize_manifest_files
from .update_manifest_contents import update_manifest_contents

TASK_MODULES = [
    filter_manifest_files,
    find_missing_conditions,
    summarize_manifest_files,
    update_manifest_contents,
]

for task_module in TASK_MODULES:
    MODULE_NAME = task_module.__name__
    module = importlib.import_module(f".{MODULE_NAME}", package=__name__)
    setattr(sys.modules[__name__], MODULE_NAME, task(getattr(module, MODULE_NAME)))
