import unittest

import pandas as pd
from tabulate import tabulate

from container_collection.manifest.summarize_manifest_files import summarize_manifest_files


class TestSummarizeManifestFiles(unittest.TestCase):
    def setUp(self) -> None:
        self.name = "name"
        self.seeds = [0, 1, 2, 3]
        self.conditions = [{"key": "key_A"}, {"key": "key_B"}, {"key": "key_C"}]
        self.summary_index = ["name_key_A", "name_key_B", "name_key_C"]

    def test_summarize_manifest_files_all_key_extension_pairs_with_seeds_and_no_seeds(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_A_0001",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0003",
            "name_key_C_0001",
            "name_key_C_0002",
            "name_key_C_0003",
            "name_key_A",
            "name_key_B",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["1/4 (25.00 %)", "2/4 (50.00 %)", "1/4 (25.00 %)"]
        extension_b_summary = ["1/4 (25.00 %)", "3/4 (75.00 %)", "2/4 (50.00 %)"]
        extension_c_summary = ["✓", "✓", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_all_key_extension_pairs_only_with_seeds(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_A_0001",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0003",
            "name_key_C_0001",
            "name_key_C_0002",
            "name_key_C_0003",
            "name_key_A_0003",
            "name_key_B_0002",
            "name_key_C_0001",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_b",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["1/4 (25.00 %)", "2/4 (50.00 %)", "1/4 (25.00 %)"]
        extension_b_summary = ["1/4 (25.00 %)", "3/4 (75.00 %)", "2/4 (50.00 %)"]
        extension_c_summary = ["1/4 (25.00 %)", "1/4 (25.00 %)", "1/4 (25.00 %)"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_all_key_extension_pairs_only_with_no_seeds(self):
        manifest_keys = [
            "name_key_A",
            "name_key_B",
            "name_key_C",
            "name_key_A",
            "name_key_B",
            "name_key_C",
            "name_key_A",
            "name_key_B",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["✓", "✓", "✓"]
        extension_b_summary = ["✓", "✓", "✓"]
        extension_c_summary = ["✓", "✓", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_key_extension_pairs_with_seeds_and_or_without_seeds(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0003",
            "name_key_A",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["1/4 (25.00 %)", "2/4 (50.00 %)", ""]
        extension_b_summary = ["", "3/4 (75.00 %)", ""]
        extension_c_summary = ["✓", "", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_manifest_contains_extraneous_columns(self):
        manifest_keys = [
            "name_key_A_0000",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0003",
            "name_key_A",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
        ]
        manifest_extraneous = [
            "extraneous",
            "extraneous",
            "extraneous",
            "extraneous",
            "extraneous",
            "extraneous",
            "extraneous",
            "extraneous",
        ]
        manifest = pd.DataFrame(
            data={
                "KEY": manifest_keys,
                "EXTENSION": manifest_extensions,
                "EXTRANEOUS": manifest_extraneous,
            }
        )
        extension_a_summary = ["1/4 (25.00 %)", "2/4 (50.00 %)", ""]
        extension_b_summary = ["", "3/4 (75.00 %)", ""]
        extension_c_summary = ["✓", "", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_manifest_contains_keys_with_unspecified_names(self):
        manifest_keys = [
            "name3_key_A_0000",
            "name2_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name2_key_B_0001",
            "name_key_B_0003",
            "name_key_A",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["", "1/4 (25.00 %)", ""]
        extension_b_summary = ["", "2/4 (50.00 %)", ""]
        extension_c_summary = ["✓", "", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_manifest_contains_keys_with_unspecified_conditions(self):
        manifest_keys = [
            "name_key_D_0000",
            "name_key_D_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_D_0001",
            "name_key_B_0003",
            "name_key_A",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["", "1/4 (25.00 %)", ""]
        extension_b_summary = ["", "2/4 (50.00 %)", ""]
        extension_c_summary = ["✓", "", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)

    def test_summarize_manifest_files_manifest_contains_keys_with_unspecified_seeds(self):
        manifest_keys = [
            "name_key_A_0005",
            "name_key_B_0001",
            "name_key_B_0002",
            "name_key_B_0000",
            "name_key_B_0001",
            "name_key_B_0003",
            "name_key_A_0005",
            "name_key_C",
        ]
        manifest_extensions = [
            "extension_a",
            "extension_a",
            "extension_a",
            "extension_b",
            "extension_b",
            "extension_b",
            "extension_c",
            "extension_c",
        ]
        manifest = pd.DataFrame(data={"KEY": manifest_keys, "EXTENSION": manifest_extensions})
        extension_a_summary = ["", "2/4 (50.00 %)", ""]
        extension_b_summary = ["", "3/4 (75.00 %)", ""]
        extension_c_summary = ["", "", "✓"]
        expected_summary_frame = pd.DataFrame(
            data={
                "extension_a": extension_a_summary,
                "extension_b": extension_b_summary,
                "extension_c": extension_c_summary,
            },
            index=self.summary_index,
        )
        expected_summary = tabulate(
            expected_summary_frame, headers="keys", tablefmt="mixed_outline"
        )
        actual_summary = summarize_manifest_files(
            manifest=manifest, name=self.name, conditions=self.conditions, seeds=self.seeds
        )
        self.assertEqual(expected_summary, actual_summary)
