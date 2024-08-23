import re

import pandas as pd
from tabulate import tabulate


def summarize_manifest_files(
    manifest: pd.DataFrame, name: str, conditions: list[dict], seeds: list[int]
) -> str:
    """
    Summarize manifest files into a table.

    Summary table is formatted as:

    .. code-block:: bash

        ┍━━━━━━━━━━━━━━━━━┯━━━━━━━━━━━━━━━┯━━━━━┯━━━━━━━━━━━━━┑
        │                 │ extension.a   │ ... │ extension.n │
        ┝━━━━━━━━━━━━━━━━━┿━━━━━━━━━━━━━━━┿━━━━━┿━━━━━━━━━━━━━┥
        │ condition_key_a │ #/# (##.## %) │ ... │ ✓           │
        │ condition_key_b │ #/# (##.## %) │ ... │ ✓           │
        │ ...             │...            │ ... │ ...         │
        │ condition_key_n │ #/# (##.## %) │ ... │ ✓           │
        ┕━━━━━━━━━━━━━━━━━┷━━━━━━━━━━━━━━━┷━━━━━┷━━━━━━━━━━━━━┙

    For file extensions that have files  or each random seed, the summary table
    reports the number and percentage of random seeds. For file extensions with
    only one file per conditions, a checkmark (✓) is used to indicate if the
    file exists or not.

    Parameters
    ----------
    manifest
        Manifest of file keys, extensions, and locations.
    name
        Name of the simulation series.
    conditions
        List of series condition dictionaries (must include unique condition
        "key").
    seeds
        List of series random seeds.

    Returns
    -------
    :
        Manifest summary table.
    """

    condition_keys = [f"{name}_{condition['key']}" for condition in conditions]
    manifest_keys = manifest.set_index("KEY").filter(regex=f"^{name}", axis="index").reset_index()
    extensions = manifest_keys["EXTENSION"].unique()

    counts = pd.DataFrame(index=condition_keys, columns=extensions).fillna(0)
    summary = pd.DataFrame(index=condition_keys, columns=extensions)

    for entry in manifest_keys.to_dict("records"):
        key = entry["KEY"]
        extension = entry["EXTENSION"]

        match = re.search("[0-9]{4}", key)
        if match:
            key = entry["KEY"].replace(f"_{match.group(0)}", "")
            if key in condition_keys and int(match.group(0)) in seeds:
                counts.loc[key][extension] += 1
                count = counts.loc[key][extension]
                percent = count / len(seeds) * 100
                summary.loc[key, extension] = f"{count}/{len(seeds)} ({percent:.2f} %)"
        elif key in condition_keys:
            summary.loc[key, extension] = "✓"

    summary.dropna(axis=1, how="all", inplace=True)
    summary.fillna("", inplace=True)

    return tabulate(summary, headers="keys", tablefmt="mixed_outline")
