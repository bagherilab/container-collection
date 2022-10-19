import os

import pandas as pd
from prefect import task


@task
def update_manifest_contents(location_files: dict) -> pd.DataFrame:
    all_manifests = []

    for location, files in location_files.items():
        location_manifest = make_file_manifest(location, files)
        all_manifests.append(location_manifest)

    manifest = pd.concat(all_manifests)
    manifest.sort_values(by=["EXTENSION", "KEY"], inplace=True)
    manifest.reset_index(drop=True, inplace=True)

    return manifest


def make_file_manifest(location: str, files: list[str]) -> pd.DataFrame:
    contents = []

    for file in files:
        key = os.path.split(file)[1].split(".")[0]
        extension = ".".join(os.path.split(file)[1].split(".")[1:])
        contents.append((key, extension, os.path.join(location, file)))

    manifest = pd.DataFrame(contents, columns=["KEY", "EXTENSION", "LOCATION"])

    return manifest
