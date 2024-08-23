from fnmatch import fnmatch

import pandas as pd


def filter_manifest_files(
    manifest: pd.DataFrame,
    extensions: list[str],
    include_filters: list[str],
    exclude_filters: list[str],
) -> dict:
    """
    Filter manifest file keys by incomplete extensions and given filters.

    Parameters
    ----------
    manifest
        Manifest of file keys, extensions, and locations.
    extensions
        List of single simulation output extensions.
    include_filters
        List of Unix filename pattern matching filter for included file keys.
    exclude_filters
        List of Unix filename pattern matching filter for excluded file keys.

    Returns
    -------
    :
        Map of filtered manifest file keys to extensions and locations.
    """

    complete_manifest = filter_incomplete_extensions(manifest, extensions)
    manifest_files = convert_to_dictionary(complete_manifest)
    selected_keys = filter_file_keys(list(manifest_files.keys()), include_filters, exclude_filters)

    return {key: manifest_files[key] for key in selected_keys}


def filter_incomplete_extensions(manifest: pd.DataFrame, extensions: list[str]) -> pd.DataFrame:
    """
    Filters manifest for files with incomplete set of extensions.

    Parameters
    ----------
    manifest
        Manifest of file keys, extensions, and locations.
    extensions
        List of single simulation output extensions.

    Returns
    -------
    :
        Filtered manifest of file keys, extensions, and locations.
    """

    filtered = manifest.groupby("KEY").filter(
        lambda x: len(set(extensions) - set(x["EXTENSION"])) == 0
    )
    return filtered


def convert_to_dictionary(manifest: pd.DataFrame) -> dict:
    """
    Convert manifest dataframe to map of file key to extensions and locations.

    Parameters
    ----------
    manifest
        Manifest of file keys, extensions, and locations.

    Returns
    -------
    :
        Map of file key to extensions and locations.
    """

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


def filter_file_keys(
    files: list[str], include_filters: list[str], exclude_filters: list[str]
) -> set:
    """
    Filter keys using include and exclude Unix filename pattern matching filters.

    Parameters
    ----------
    files
        List of file keys.
    include_filters
        List of Unix filename pattern matching filter for included file keys.
    exclude_filters
        List of Unix filename pattern matching filter for excluded file keys.

    Returns
    -------
    :
        Filtered set of file keys.
    """

    # Filter key list by include and exclude filters.
    selected_keys = set()
    unselected_keys = set()

    # Filter files for matches to include filters.
    for include in include_filters:
        selected_keys.update([key for key in files if fnmatch(key, include)])

    # Filter files for matches to exclude filters.
    for exclude in exclude_filters:
        unselected_keys.update([key for key in files if fnmatch(key, exclude)])

    # Remove unselected keys from selected keys.
    return selected_keys - unselected_keys
