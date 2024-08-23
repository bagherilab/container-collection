import unittest

import pandas as pd

from container_collection.manifest.filter_manifest_files import filter_manifest_files


class TestFilterManifestFiles(unittest.TestCase):
    def setUp(self) -> None:
        self.extensions = ["extension_a", "extension_b"]
        self.manifest_columns = ["KEY", "EXTENSION", "LOCATION", "FULL_KEY"]

    def test_filter_manifest_files_empty_manifest(self):
        manifest = pd.DataFrame([], columns=self.manifest_columns)
        include_filters = []
        exclude_filters = []
        expected_selection = {}
        actual_selection = filter_manifest_files(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

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
        actual_selection = filter_manifest_files(
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
        actual_selection = filter_manifest_files(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_key_with_incomplete_extensions(self):
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
        actual_selection = filter_manifest_files(
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
        actual_selection = filter_manifest_files(
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
        actual_selection = filter_manifest_files(
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
        actual_selection = filter_manifest_files(
            manifest=manifest,
            extensions=self.extensions,
            include_filters=include_filters,
            exclude_filters=exclude_filters,
        )
        self.assertEqual(expected_selection, actual_selection)

    def test_filter_manifest_files_include_and_exclude_filters(self):
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

        parameters = [
            (
                ["*B*"],
                ["*_0000"],
                {
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
                },
            ),
            (
                ["*_0000"],
                ["*B*"],
                {
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
                },
            ),
        ]

        for include_filters, exclude_filters, expected_selection in parameters:
            with self.subTest(include=include_filters, exclude=exclude_filters):
                actual_selection = filter_manifest_files(
                    manifest=manifest,
                    extensions=self.extensions,
                    include_filters=include_filters,
                    exclude_filters=exclude_filters,
                )
                self.assertEqual(expected_selection, actual_selection)
