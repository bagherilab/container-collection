import os

import pandas as pd


def update_manifest_contents(location_keys: dict) -> pd.DataFrame:
    """
    Update manifest using files at given keys at specified locations.

    Parameters
    ----------
    location_keys
        Map of locations to list of file keys.

    Returns
    -------
    :
        Combined manifest of file keys, extensions, and locations.
    """

    all_manifests = []

    for location, keys in location_keys.items():
        location_manifest = make_file_manifest(location, keys)
        all_manifests.append(location_manifest)

    if len(all_manifests) == 0:
        return pd.DataFrame(columns=["KEY", "EXTENSION", "LOCATION", "FULL_KEY"])

    manifest = pd.concat(all_manifests)
    manifest.sort_values(by=["EXTENSION", "KEY"], inplace=True)
    manifest.reset_index(drop=True, inplace=True)

    return manifest


def make_file_manifest(location: str, keys: list[str]) -> pd.DataFrame:
    """
    Create manifest for location with given list of file keys.

    Parameters
    ----------
    location
        File location (local path or S3 bucket).
    keys
        List of file keys.

    Returns
    -------
    :
        Manifest of file keys, extensions, and locations.
    """

    contents = []

    for key in keys:
        short_key = os.path.split(key)[1].split(".")[0]
        extension = ".".join(os.path.split(key)[1].split(".")[1:])
        contents.append((short_key, extension, location, key))

    manifest = pd.DataFrame(contents, columns=["KEY", "EXTENSION", "LOCATION", "FULL_KEY"])

    return manifest
