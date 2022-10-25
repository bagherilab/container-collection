from prefect import task
import pandas as pd


@task
def find_missing_conditions(
    conditions: dict, seeds: list[int], extensions: list[str], manifest: pd.DataFrame
) -> list[dict]:
    missing_conditions = []

    for condition in conditions:
        all_keys = {f"{condition['key']}_{seed:04}": seed for seed in seeds}
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
