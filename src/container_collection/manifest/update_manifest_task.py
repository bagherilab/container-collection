from glob import glob
from fnmatch import fnmatch
import prefect
import pandas as pd
from prefect import Task
from prefect.utilities.aws import get_boto_client
from arcadeio.containers import ManifestContainer


class UpdateManifestTask(Task):
    """Task for summarizing status of simulation series files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, manifest: ManifestContainer):
        if prefect.context.flags["update"] is False:
            return

        # Context variables.
        file_prefix = manifest.name + "_"
        file_locations = prefect.context.file_locations

        # Get all files from given locations.
        local_files = self._get_local_files(file_prefix, file_locations)
        s3_files = self._get_s3_files(file_prefix, file_locations)

        # Convert file lists to dataframes.
        local_df = self._make_file_manifest(file_prefix, local_files)
        s3_df = self._make_file_manifest(file_prefix, s3_files)

        # Update manifest.
        man = local_df
        man = man.append(s3_df)

        # Save updated manifest.
        man = man.drop_duplicates().reset_index(drop=True)
        manifest.manifest = man.sort_values(by=["EXTENSION", "KEY"])
        manifest.manifest.to_csv(manifest.file, index=False)

    @staticmethod
    def _get_local_files(file_prefix, file_locations):
        files = []
        folders = [location for location in file_locations if location[0:5] != "s3://"]

        for folder in folders:
            files = files + glob(folder + "**/" + file_prefix + "*", recursive=True)

        return files

    @staticmethod
    def _get_s3_files(file_prefix, file_locations):
        files = []
        buckets = [location for location in file_locations if location[0:5] == "s3://"]

        # Return empty list if no buckets to check.
        if len(buckets) == 0:
            return files

        # Get S3 client.
        s3_client = get_boto_client("s3")

        for bucket in buckets:
            bucket_split = bucket.replace("s3://", "").split("/")
            bucket_name = bucket_split[0]
            bucket_prefix = "/".join(bucket_split[1:])

            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)
            get_response = True

            while get_response:
                if "Contents" not in response:
                    break
                all_keys = [content["Key"] for content in response["Contents"]]
                keys = [key for key in all_keys if fnmatch(key.split("/")[-1], file_prefix + "*")]
                files = files + [f"s3://{bucket_name}/{key}" for key in keys]

                if response["IsTruncated"]:
                    response = s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=bucket_prefix,
                        ContinuationToken=response["NextContinuationToken"],
                    )
                else:
                    get_response = False

        return files

    @staticmethod
    def _make_file_manifest(file_prefix, file_list):
        contents = []

        for file in file_list:
            name = file.split("/")[-1]
            key = name.split(".")[0].replace(file_prefix, "")
            extension = ".".join(name.split(".")[1:])
            contents.append((key, extension, file))

        manifest = pd.DataFrame(contents, columns=["KEY", "EXTENSION", "LOCATION"])
        return manifest
