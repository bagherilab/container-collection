import os.path
import pandas as pd
from prefect import Task
from arcadeio.containers import ManifestContainer


class LoadManifestTask(Task):
    """Task for loading manifest file into container."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, manifest_file: str) -> ManifestContainer:
        # Load manifest file.
        manifest_contents = LoadManifestTask._load_manifest_contents(manifest_file)

        # Parse file name to get manifest name.
        manifest_name = LoadManifestTask._parse_manifest_name(manifest_file)

        return ManifestContainer(manifest_name, manifest_file, manifest_contents)

    @staticmethod
    def _load_manifest_contents(manifest_file):
        dtypes = {
            "KEY": "object",
            "EXTENSION": "object",
            "LOCATION": "object",
        }
        contents = pd.DataFrame(columns=["KEY", "EXTENSION", "LOCATION"])

        if manifest_file[0:5] == "s3://":
            # TODO: check and load from s3
            pass
        elif os.path.isfile(manifest_file):
            contents = pd.read_csv(manifest_file, dtype=dtypes)

        return contents

    @staticmethod
    def _parse_manifest_name(manifest_file):
        split_manifest_file = manifest_file.split("/")
        manifest_name = split_manifest_file[-1].replace(".csv", "")
        return manifest_name
