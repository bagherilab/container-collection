import re
from prefect import Task
import pandas as pd
from arcadeio.containers import SeriesContainer, ManifestContainer
from arcadeio.containers.summarize import SummaryContainer
from arcadeio.expressions import SEED_MATCH


class SummarizeFilesTask(Task):
    """Task for summarizing status of simulation series files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, series: SeriesContainer, manifest: ManifestContainer) -> SummaryContainer:
        # Get all possible names for summary index.
        index = [n for n, c in series.names]

        # Get all extensions for summary columns.
        extensions = list(set(manifest.manifest.EXTENSION.tolist()))

        # Create summary table.
        summary_table = self._create_summary_table(manifest.manifest, index, extensions)

        return SummaryContainer(summary_table)

    @staticmethod
    def _create_summary_table(manifest_df, index, extensions):
        # Create data frame.
        summary_table = pd.DataFrame(0, columns=extensions, index=index).astype("int32")
        summary_table = summary_table.sort_index()

        # Iterate through manifest to fill out the dataframe.
        for ext in extensions:
            keys = manifest_df[manifest_df.EXTENSION == ext].KEY.tolist()

            for key in keys:
                match = re.search("_" + SEED_MATCH, key)

                if match:
                    seed = match.group(0)
                    index = key.replace(seed, "")

                    if index in summary_table.index:
                        summary_table.loc[index, ext] += 1
                elif key in summary_table.index:
                    summary_table.loc[key, ext] = 1

        return summary_table
