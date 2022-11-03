import re

from prefect import task
from tabulate import tabulate
import pandas as pd


@task
def summarize_manifest_files(
    manifest: pd.DataFrame, name: str, conditions: dict, seeds: list[str]
) -> str:
    condition_keys = [condition["key"] for condition in conditions]
    manifest_keys = manifest.set_index("KEY").filter(regex=f"^{name}", axis="index").reset_index()
    extensions = manifest_keys["EXTENSION"].unique()

    counts = pd.DataFrame(index=condition_keys, columns=extensions).fillna(0)
    summary = pd.DataFrame(index=condition_keys, columns=extensions)

    for entry in manifest_keys.to_dict("records"):
        key = entry["KEY"]
        extension = entry["EXTENSION"]

        match = re.search("_[0-9]{4}", key)
        if match:
            key = entry["KEY"].replace(match.group(0), "")
            if key in condition_keys:
                counts.loc[key][extension] += 1
                count = counts.loc[key][extension]
                percent = count / len(seeds) * 100
                summary.loc[key, extension] = f"{count}/{len(seeds)} ({percent:.2f} %)"
        elif key in condition_keys:
            summary.loc[key, extension] = "✓"

    summary.dropna(axis=1, how="all", inplace=True)
    summary.fillna("", inplace=True)

    return tabulate(summary, headers="keys", tablefmt="mixed_outline")
