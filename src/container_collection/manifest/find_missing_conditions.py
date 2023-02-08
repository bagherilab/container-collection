import pandas as pd
from prefect import task


@task
def find_missing_conditions(
    manifest: pd.DataFrame,
    name: str,
    conditions: list[dict],
    seeds: list[int],
    extensions: list[str],
) -> list[dict]:
    missing_conditions = []

    for condition in conditions:
        all_keys = {f"{name}_{condition['key']}_{seed:04}": seed for seed in seeds}
        manifest_keys = manifest[manifest["KEY"].isin(all_keys.keys())]

        missing_keys = set(all_keys) - set(manifest_keys["KEY"].unique())
        incomplete_keys = set(
            manifest_keys.groupby("KEY").filter(
                lambda x: len(set(extensions) - set(x["EXTENSION"])) != 0
            )["KEY"]
        )

        for key in list(missing_keys) + list(incomplete_keys):
            missing_condition = condition.copy()
            missing_condition["seed"] = all_keys[key]
            missing_conditions.append(missing_condition)

    return missing_conditions
