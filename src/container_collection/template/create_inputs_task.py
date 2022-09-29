import re
from itertools import groupby
from functools import reduce
import prefect
from prefect import Task
from arcadeio.containers import SeriesContainer
from arcadeio.containers.simulate import SeedContainer, InputContainer
from arcadeio.expressions import SERIES_NAME_ATTRIBUTE, START_SEED_REGEX, END_SEED_REGEX


class CreateInputsTask(Task):
    """Task for creating job input files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, series: SeriesContainer, seeds: SeedContainer) -> InputContainer:
        # Context variables.
        seeds_per_job = prefect.context.seeds_per_job

        # Process template contents for building setup files
        contents, header, footer = self._extract_series_contents(series.contents)

        # Create setup files for missing seeds.
        missing_seeds = self._group_missing_seeds(seeds.names, seeds.missing, seeds_per_job)
        series_blocks = self._build_series_blocks(contents, missing_seeds, series.replace)

        # Create input files by grouping series blocks.
        input_contents = self._make_input_contents(series_blocks, seeds_per_job)

        return InputContainer(header, footer, input_contents)

    @staticmethod
    def _extract_series_contents(contents):
        header = re.search(r'<set[A-z_/0-9\s"=~]*>\n', contents).group(0)
        footer = "\n</set>"
        extracted_contents = contents.replace(header, "")
        extracted_contents = extracted_contents.replace(footer, "")
        return extracted_contents, header, footer

    @staticmethod
    def _group_missing_seeds(missing_names, missing_seeds, seeds):
        missing_ranges = [
            (name, combo, CreateInputsTask._find_seed_ranges(missing, seeds))
            for (name, combo), missing in zip(missing_names, missing_seeds)
        ]
        missing_seed_groups = [(a, b, str(d), str(e)) for a, b, c in missing_ranges for d, e in c]
        return missing_seed_groups

    @staticmethod
    def _build_series_blocks(contents, missing_seed_groups, replacements):
        name_attribute = re.search(SERIES_NAME_ATTRIBUTE, contents).group(1)

        # Compile input files.
        series_blocks = []
        for name, combo, start, end in missing_seed_groups:
            series_block = contents.replace(name_attribute, name)
            series_block = START_SEED_REGEX.sub('start="' + str(start) + '"', series_block)
            series_block = END_SEED_REGEX.sub('end="' + str(end) + '"', series_block)
            series_block = reduce(lambda a, kv: a.replace(*kv), replacements[combo], series_block)
            series_blocks.append((series_block, int(end) - int(start) + 1))

        return series_blocks

    @staticmethod
    def _find_seed_ranges(seeds, max_jobs):
        sort = sorted([int(x) for x in seeds])
        ranges = []
        for _, group in groupby(enumerate(sort), lambda x: x[0] - x[1]):
            group_list = list(group)
            subset = True
            range_start = group_list[0][1]
            range_end = group_list[-1][1]

            while subset:
                ranges.append((range_start, min(range_end, range_start + max_jobs - 1)))
                range_start = range_start + max_jobs
                if range_start > range_end:
                    subset = False

        return ranges

    @staticmethod
    def _make_input_contents(series_blocks, seeds):
        num = 0
        inputs = []
        input_contents = []

        for content, count in series_blocks:
            if num + count <= seeds:
                inputs.append(content)
                num = num + count
            else:
                input_contents.append(inputs)
                num = count
                inputs = [content]
        input_contents.append(inputs)

        return input_contents
