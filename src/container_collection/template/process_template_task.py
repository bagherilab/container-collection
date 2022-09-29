import re
from itertools import product
from functools import reduce
import numpy as np
from prefect import Task
from arcadeio.containers import TemplateContainer, SeriesContainer
from arcadeio.expressions import (
    SERIES_NAME_ATTRIBUTE,
    START_SEED_ATTRIBUTE,
    END_SEED_ATTRIBUTE,
    TAG_MATCH,
    OPTIONS_MATCH,
    TEMPLATE_NAME_PATTERN,
    TEMPLATE_PADDING_PATTERN,
)


class ProcessTemplateTask(Task):
    """Task for processing template container into series."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, template: TemplateContainer) -> SeriesContainer:
        # Parse template seeds.
        start_seed, end_seed = self._parse_template_seeds(template.template)

        # Extract tags, options, and combinations of tag options
        all_tags, all_options = self._extract_template_entries(template.template)
        unique_tags, tag_groups = self._group_unique_tags(all_tags)
        ordered_tags, ordered_options = self._sort_tag_options(all_options, unique_tags, tag_groups)
        combos = self._create_option_combos(ordered_options)

        # Update template contents to assign ids to grouped tags.
        contents = self._update_template_contents(template.template, unique_tags, tag_groups)
        replace = self._make_entry_replacements(ordered_tags, combos)

        # Create all names.
        all_names = self._make_series_names(template.template, combos, replace)

        return SeriesContainer(start_seed, end_seed, contents, replace, all_names)

    @staticmethod
    def _parse_template_seeds(xml):
        # Search for start and end seed.
        start_search = re.search(START_SEED_ATTRIBUTE, xml)
        end_search = re.search(END_SEED_ATTRIBUTE, xml)

        # Set seeds, default is 0.
        start_seed = int(start_search.group(1)) if start_search is not None else 0
        end_seed = int(end_search.group(1)) if end_search is not None else 0

        return start_seed, end_seed

    @staticmethod
    def _extract_template_entries(xml):
        # Search through template for all {tag::options} entries.
        all_tags = []
        all_options = []
        for row in xml.split(r"\n"):
            matches = re.findall(r"\{(" + TAG_MATCH + "::" + OPTIONS_MATCH + r")\}", row)
            if matches:
                for match in matches:
                    all_tags.append(match[1])
                    all_options.append(match[2].replace("[", "<!--").replace("]", "-->").split("|"))
        return all_tags, all_options

    @staticmethod
    def _group_unique_tags(all_tags):
        # Get unique tags. Previously used set(tags), but want to have consistent order.
        unique_tags = []
        for tag in all_tags:
            if tag not in unique_tags:
                unique_tags.append(tag)

        # Sort for grouped tags.
        tag_groups = {p: [i for i, t in enumerate(all_tags) if t == p] for p in unique_tags}

        return unique_tags, tag_groups

    @staticmethod
    def _sort_tag_options(all_options, unique_tags, tag_groups):
        # Organize options and consolidate into tuples for grouped tags.
        options = []
        for tag in unique_tags:
            if len(tag_groups[tag]) == 1:
                options.append(all_options[tag_groups[tag][0]])
            else:
                ops = [all_options[i] for i in tag_groups[tag]]
                options.append(list(zip(*ops)))

        # Sort tags and options.
        ordering = list(np.argsort([len(op) for op in options]))
        ordering.reverse()
        ordered_options = [options[i] for i in ordering]
        ordered_tags = [unique_tags[i] for i in ordering]

        return ordered_tags, ordered_options

    @staticmethod
    def _create_option_combos(options):
        combos = list(product(*options))
        return combos

    @staticmethod
    def _update_template_contents(xml, unique_tags, tag_groups):
        contents = "".join(xml)
        for tag in unique_tags:
            if len(tag_groups[tag]) == 1:
                contents = re.sub(r"\{" + f"{tag}::{OPTIONS_MATCH}" + r"\}", f"{{{tag}}}", contents)
            else:
                for i in range(len(tag_groups[tag])):
                    contents = re.sub(
                        r"\{" + f"{tag}::{OPTIONS_MATCH}" + r"\}",
                        f"{{{tag}_{str(i)}}}",
                        contents,
                        1,
                    )
        return contents

    @staticmethod
    def _make_entry_replacements(tags, combos):
        replacements = {}
        for combo in combos:
            singles = [("{" + t + "}", c) for t, c in zip(tags, combo) if not isinstance(c, tuple)]
            grouped = [
                ("{" + t + "_" + str(i) + "}", cc)
                for t, c in zip(tags, combo)
                for i, cc in enumerate(c)
                if isinstance(c, tuple)
            ]
            replacements[combo] = singles + grouped
        return replacements

    @staticmethod
    def _make_series_names(xml, combos, replace):
        # Get name template.
        name_template = re.search(SERIES_NAME_ATTRIBUTE, xml).group(1)

        # Iterate through combinations to make names
        series_names = []
        for combo in combos:
            padding = re.findall(TEMPLATE_NAME_PATTERN, name_template)
            pads = {"{" + p[0] + "}": (int(p[1]), int(p[2])) for p in padding}
            template = re.sub(TEMPLATE_PADDING_PATTERN, "", name_template)
            series_name = reduce(
                lambda a, kv: a.replace(*kv),
                [ProcessTemplateTask._clean_series_name(k, v, pads) for k, v in replace[combo]],
                template,
            )
            series_names.append((series_name, combo))
        return series_names

    @staticmethod
    def _clean_series_name(key, value, pads):
        if key in pads.keys() and value.replace(".", "", 1).isdigit():
            value = str(int(float(value) * pads[key][0])).zfill(pads[key][1])
        value = value.replace(",", "")
        return (key, value)
