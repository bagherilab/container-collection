import os

import pandas as pd
from prefect import task


@task
def update_manifest_contents(location_keys: dict) -> pd.DataFrame:
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
    contents = []

    for key in keys:
        short_key = os.path.split(key)[1].split(".")[0]
        extension = ".".join(os.path.split(key)[1].split(".")[1:])
        contents.append((short_key, extension, location, key))

    manifest = pd.DataFrame(contents, columns=["KEY", "EXTENSION", "LOCATION", "FULL_KEY"])

    return manifest
