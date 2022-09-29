import re
from prefect import Task
import prefect
from arcadeio.containers import SeriesContainer, ManifestContainer
from arcadeio.containers.simulate import SeedContainer
from arcadeio.expressions import SEED_MATCH


class FindMissingTask(Task):
    """Task for summarizing status of simulation series files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, series: SeriesContainer, manifest: ManifestContainer) -> SeedContainer:
        # Get series simulation settings.
        seeds = self._make_seed_set(series.start_seed, series.end_seed)

        # Find missing seeds by checking manifest.
        key_list = self._get_key_list(manifest.manifest, prefect.context.file_extensions)
        missing_names, missing_seeds = self._find_missing_seeds(series.names, seeds, key_list)

        return SeedContainer(missing_names, missing_seeds)

    @staticmethod
    def _make_seed_set(start_seed, end_seed):
        # Make set for all seeds.
        full_seed_set = set(list(range(start_seed, end_seed + 1)))
        return full_seed_set

    @staticmethod
    def _get_key_list(manifest_df, extensions):
        all_keys = []

        for ext in extensions:
            keys = manifest_df[manifest_df.EXTENSION == ext].KEY.tolist()
            all_keys = all_keys + keys

        return list(set(all_keys))

    @staticmethod
    def _find_missing_seeds(series_names, full_seed_set, key_list):
        missing_names = []
        missing_seeds = []

        # Iterate through all names and find matches.
        for name, combo in series_names:
            name_pattern = name + "_" + SEED_MATCH
            matches = [m.group(0) for x in key_list for m in [re.search(name_pattern, x)] if m]
            seeds = {int(m.group(0)) for x in matches for m in [re.search(SEED_MATCH, x)] if m}
            missing = list(full_seed_set - seeds)

            if missing:
                missing_names.append((name, combo))
                missing_seeds.append(missing)

        return missing_names, missing_seeds
