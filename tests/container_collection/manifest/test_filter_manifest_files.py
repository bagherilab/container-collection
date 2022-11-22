import unittest
from container_collection.manifest.filter_manifest_files import filter_manifest_files
import pandas as pd


class TestFilterManifestFiles(unittest.TestCase):
    def setUp(self) -> None:
        self.extensions = ["extension_a", "extension_b"]
        self.manifest_columns = ["KEY", "EXTENSION", "LOCATION", "FULL_KEY"]

    def test_filter_manifest_files_include_all_keys(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*"]
        exclude_filters = []
        expected_selection = {
            "name_key_A_0000": {
                "extension_a": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_a",
                },
                "extension_b": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_b",
                },
            },
            "name_key_A_0001": {
                "extension_a": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0001.extension_a",
                },
                "extension_b": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0001.extension_b",
                },
            },
            "name_key_B_0000": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0000.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0000.extension_b",
                },
            },
            "name_key_B_0001": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_exclude_all_keys(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = []
        exclude_filters = ["*"]
        expected_selection = {}
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_include_all_keys_key_with_incomplete_extensions(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*"]
        exclude_filters = []
        expected_selection = {
            "name_key_A_0000": {
                "extension_a": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_a",
                },
                "extension_b": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_b",
                },
            },
            "name_key_B_0001": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_same_include_exclude_filters(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*_0001"]
        exclude_filters = ["*_0001"]
        expected_selection = {}
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_multiple_include_filters(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
            [
                "name_key_C_0002",
                "extension_a",
                "/local/path/",
                "full/file/name_key_C_0002.extension_a",
            ],
            [
                "name_key_C_0002",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_C_0002.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*C*", "*_0002"]
        exclude_filters = []
        expected_selection = {
            "name_key_C_0002": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_C_0002.extension_a",
                },
                "extension_b": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_C_0002.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_multiple_exclude_filters(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
            [
                "name_key_C_0002",
                "extension_a",
                "/local/path/",
                "full/file/name_key_C_0002.extension_a",
            ],
            [
                "name_key_C_0002",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_C_0002.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*"]
        exclude_filters = ["*A*", "*C*"]
        expected_selection = {
            "name_key_B_0000": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0000.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0000.extension_b",
                },
            },
            "name_key_B_0001": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_include_B_exclude_0000(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*B*"]
        exclude_filters = ["*_0000"]
        expected_selection = {
            "name_key_B_0001": {
                "extension_a": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_a",
                },
                "extension_b": {
                    "location": "/local/path/",
                    "key": "full/file/name_key_B_0001.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_include_0000_exclude_B(self):
        contents = [
            [
                "name_key_A_0000",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_a",
            ],
            [
                "name_key_A_0000",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0000.extension_b",
            ],
            [
                "name_key_A_0001",
                "extension_a",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_a",
            ],
            [
                "name_key_A_0001",
                "extension_b",
                "s3://bucket-name",
                "full/file/name_key_A_0001.extension_b",
            ],
            [
                "name_key_B_0000",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0000.extension_a",
            ],
            [
                "name_key_B_0000",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0000.extension_b",
            ],
            [
                "name_key_B_0001",
                "extension_a",
                "/local/path/",
                "full/file/name_key_B_0001.extension_a",
            ],
            [
                "name_key_B_0001",
                "extension_b",
                "/local/path/",
                "full/file/name_key_B_0001.extension_b",
            ],
        ]
        manifest = pd.DataFrame(contents, columns=self.manifest_columns)
        include_filters = ["*_0000"]
        exclude_filters = ["*B*"]
        expected_selection = {
            "name_key_A_0000": {
                "extension_a": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_a",
                },
                "extension_b": {
                    "location": "s3://bucket-name",
                    "key": "full/file/name_key_A_0000.extension_b",
                },
            },
        }
        actual_selection = filter_manifest_files.fn(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)
