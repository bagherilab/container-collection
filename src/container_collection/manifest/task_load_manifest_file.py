import os

import pandas as pd
from prefect import task

from container_collection.manifest import ManifestContainer


@task
def load_manifest_file(manifest_file: str) -> ManifestContainer:
    """Task for loading manifest file into container."""

    dtypes = {
        "KEY": "object",
        "EXTENSION": "object",
        "LOCATION": "object",
    }

    manifest_contents = pd.DataFrame(columns=dtypes.keys())

    if manifest_file[0:5] == "s3://":
        # TODO: check and load from s3
        pass
    elif os.path.isfile(manifest_file):
        manifest_contents = pd.read_csv(manifest_file, dtype=dtypes)

    return ManifestContainer(file=manifest_file, contents=manifest_contents)
