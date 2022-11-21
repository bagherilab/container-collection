import unittest
from container_collection.manifest.find_missing_conditions import find_missing_conditions
import pandas as pd


class TestFindMissingConditions(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "name"
        self.seeds = [0, 1, 2]
        self.conditions_no_attributes = [{"key": "key_A"}, {"key": "key_B"}, {"key": "key_C"}]
        self.extensions = ["extension_a", "extension_b"]

    def test_find_missing_conditions_contains_no_missing_or_incomplete_conditions(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_A_0000",
            "name_key_A_0001",
            "name_key_A_0001",
            "name_key_A_0002",
            "name_key_A_0002",
            "name_key_B_0000",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0002",
            "name_key_C_0000",
            "name_key_C_0000",
            "name_key_C_0001",
            "name_key_C_0001",
            "name_key_C_0002",
            "name_key_C_0002",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        expected_missing = []
        actual_missing = find_missing_conditions.fn(
            manifest=manifest,
            name=self.name,
            conditions=self.conditions_no_attributes,
            seeds=self.seeds,
            extensions=self.extensions,
        )
        self.assertEqual(expected_missing, actual_missing)

    def test_find_missing_conditions_contains_only_missing_conditions(self):
        manifest_keys = [
            "name_key_A_0001",
            "name_key_A_0001",
            "name_key_A_0002",
            "name_key_A_0002",
            "name_key_B_0000",
            "name_key_B_0000",
            "name_key_B_0002",
            "name_key_B_0002",
            "name_key_C_0000",
            "name_key_C_0000",
            "name_key_C_0001",
            "name_key_C_0001",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        expected_missing = [
            {"key": "key_A", "seed": 0},
            {"key": "key_B", "seed": 1},
            {"key": "key_C", "seed": 2},
        ]
        actual_missing = find_missing_conditions.fn(
            manifest=manifest,
            name=self.name,
            conditions=self.conditions_no_attributes,
            seeds=self.seeds,
            extensions=self.extensions,
        )
        self.assertEqual(expected_missing, actual_missing)

    def test_find_missing_conditions_contains_only_incomplete_conditions(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_A_0001",
            "name_key_A_0001",
            "name_key_A_0002",
            "name_key_A_0002",
            "name_key_B_0000",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_C_0000",
            "name_key_C_0000",
            "name_key_C_0001",
            "name_key_C_0002",
            "name_key_C_0002",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_a",
            "extension_b",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        expected_missing = [
            {"key": "key_A", "seed": 0},
            {"key": "key_B", "seed": 2},
            {"key": "key_C", "seed": 1},
        ]
        actual_missing = find_missing_conditions.fn(
            manifest=manifest,
            name=self.name,
            conditions=self.conditions_no_attributes,
            seeds=self.seeds,
            extensions=self.extensions,
        )
        self.assertEqual(expected_missing, actual_missing)

    def test_find_missing_conditions_contains_missing_and_incomplete_conditions(self):
        manifest_keys = [
            "name_key_A_0001",
            "name_key_A_0002",
            "name_key_A_0002",
            "name_key_B_0000",
            "name_key_B_0000",
            "name_key_B_0002",
            "name_key_C_0000",
            "name_key_C_0001",
            "name_key_C_0001",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_a",
            "extension_b",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        expected_missing = [
            {"key": "key_A", "seed": 0},
            {"key": "key_A", "seed": 1},
            {"key": "key_B", "seed": 1},
            {"key": "key_B", "seed": 2},
            {"key": "key_C", "seed": 2},
            {"key": "key_C", "seed": 0},
        ]
        actual_missing = find_missing_conditions.fn(
            manifest=manifest,
            name=self.name,
            conditions=self.conditions_no_attributes,
            seeds=self.seeds,
            extensions=self.extensions,
        )
        self.assertEqual(expected_missing, actual_missing)

    def test_find_missing_conditions_contains_additional_condition_attributes(self):
        conditions_with_attributes = [
            {"key": "key_A", "attribute": "a"},
            {"key": "key_B", "attribute": "b"},
            {"key": "key_C", "attribute": "c"},
        ]
        manifest_keys = [
            "name_key_A_0001",
            "name_key_A_0002",
            "name_key_A_0002",
            "name_key_B_0000",
            "name_key_B_0000",
            "name_key_B_0002",
            "name_key_C_0000",
            "name_key_C_0001",
            "name_key_C_0001",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_a",
            "extension_b",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        expected_missing = [
            {"key": "key_A", "seed": 0, "attribute": "a"},
            {"key": "key_A", "seed": 1, "attribute": "a"},
            {"key": "key_B", "seed": 1, "attribute": "b"},
            {"key": "key_B", "seed": 2, "attribute": "b"},
            {"key": "key_C", "seed": 2, "attribute": "c"},
            {"key": "key_C", "seed": 0, "attribute": "c"},
        ]
        actual_missing = find_missing_conditions.fn(
            manifest=manifest,
            name=self.name,
            conditions=conditions_with_attributes,
            seeds=self.seeds,
            extensions=self.extensions,
        )
        self.assertEqual(expected_missing, actual_missing)
