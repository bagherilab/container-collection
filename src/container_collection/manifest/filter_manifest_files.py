from fnmatch import fnmatch

import pandas as pd
from prefect import task


@task
def filter_manifest_files(
    manifest: pd.DataFrame,
    extensions: list[str],
    include_filters: list[str],
    exclude_filters: list[str],
) -> dict:
    complete_manifest = filter_incomplete_extensions(manifest, extensions)
    manifest_files = convert_to_dictionary(complete_manifest)
    selected_keys = filter_file_keys(manifest_files, include_filters, exclude_filters)

    return {key: manifest_files[key] for key in selected_keys}


def filter_incomplete_extensions(manifest: pd.DataFrame, extensions: list[str]) -> pd.DataFrame:
    filtered = manifest.groupby("KEY").filter(
        lambda x: len(set(extensions) - set(x["EXTENSION"])) == 0
    )
    return filtered


def convert_to_dictionary(manifest: pd.DataFrame) -> dict:
    # Return empty dictionary if there are no entries.
    if manifest.empty:
        return {}

    # Convert into dictionary of dictionaries.
    manifest_dict = {}

    for key, group in manifest.groupby("KEY"):
        converted = group.to_dict("records")
        manifest_dict[key] = {
            x["EXTENSION"]: {"location": x["LOCATION"], "key": x["FULL_KEY"]} for x in converted
        }

    return manifest_dict


def filter_file_keys(files: dict, include_filters: list[str], exclude_filters: list[str]) -> set:
    # Filter key list by include and exclude filters.
    selected_keys = set()
    unselected_keys = set()
    all_keys = list(files.keys())

    # Filter files for matches to include filters.
    for include in include_filters:
        selected_keys.update([key for key in all_keys if fnmatch(key, include)])

    # Filter files for matches to exclude filters.
    for exclude in exclude_filters:
        unselected_keys.update([key for key in all_keys if fnmatch(key, exclude)])

    # Remove unselected keys from selected keys.
    return selected_keys - unselected_keys
