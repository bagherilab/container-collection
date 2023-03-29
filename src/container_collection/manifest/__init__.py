from prefect import task

from .filter_manifest_files import filter_manifest_files
from .find_missing_conditions import find_missing_conditions
from .summarize_manifest_files import summarize_manifest_files
from .update_manifest_contents import update_manifest_contents

filter_manifest_files = task(filter_manifest_files)
find_missing_conditions = task(find_missing_conditions)
summarize_manifest_files = task(summarize_manifest_files)
update_manifest_contents = task(update_manifest_contents)
