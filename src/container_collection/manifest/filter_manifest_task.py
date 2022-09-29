from fnmatch import fnmatch
import prefect
from prefect import Task
from arcadeio.containers import ManifestContainer


class FilterManifestTask(Task):
    """Task for selecting output files to parse."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, manifest: ManifestContainer):
        # Context variables.
        extensions = set(prefect.context.file_extensions)
        include = prefect.context.include_filters
        exclude = prefect.context.exclude_filters

        # Filter manifest for keys with all required extensions.
        filtered_manifest = self._filter_manifest_files(manifest.manifest, extensions)
        file_dict = self._convert_to_dictionary(filtered_manifest)

        # Apply include and exclude filters.
        key_list = self._filter_file_keys(file_dict, include, exclude)
        selected_files = [(key, file_dict[key]) for key in key_list]

        return selected_files

    @staticmethod
    def _filter_manifest_files(manifest_df, extensions):
        grouped = manifest_df.groupby("KEY")
        filtered = grouped.filter(lambda x: extensions.issubset(set(x.EXTENSION)))
        return filtered

    @staticmethod
    def _convert_to_dictionary(manifest_df):
        # Return empty dictionary if there are no entries.
        if manifest_df.empty:
            return {}

        # Convert into dictionary of dictionaries.
        converted = manifest_df.groupby("KEY").apply(lambda x: x.to_dict("records")).tolist()
        file_dict = {}

        for file in converted:
            ext_dict = {x["EXTENSION"]: x["LOCATION"] for x in file}
            file_dict[file[0]["KEY"]] = ext_dict

        return file_dict

    @staticmethod
    def _filter_file_keys(file_dict, include_filters, exclude_filters):
        # Filter key list by include and exclude filters.
        selected_keys = set()
        unselected_keys = set()
        all_keys = list(file_dict.keys())

        # Filter files for matches to include filters.
        for include in include_filters:
            selected_keys.update([key for key in all_keys if fnmatch(key, include)])

        # Filter files for matches to exclude filters.
        for exclude in exclude_filters:
            unselected_keys.update([key for key in all_keys if fnmatch(key, exclude)])

        # Remove unselected keys from selected keys.
        return selected_keys - unselected_keys
