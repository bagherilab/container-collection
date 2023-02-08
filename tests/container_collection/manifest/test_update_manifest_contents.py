import unittest

import pandas as pd

from container_collection.manifest.update_manifest_contents import update_manifest_contents


class TestUpdateManifestContents(unittest.TestCase):
    def setUp(self) -> None:
        self.columns = ["KEY", "EXTENSION", "LOCATION", "FULL_KEY"]

    def test_update_manifest_contents_single_location_populated_key_list(self):
        key_list = [
            "full/file/key_1.extension_a",
            "full/file/key_2.extension_b",
            "full/file/key_3.extension_a",
        ]
        location_keys = {"s3://bucket-name": key_list}
        # df sorted by extension, then key
        contents = [
            ["key_1", "extension_a", "s3://bucket-name", "full/file/key_1.extension_a"],
            ["key_3", "extension_a", "s3://bucket-name", "full/file/key_3.extension_a"],
            ["key_2", "extension_b", "s3://bucket-name", "full/file/key_2.extension_b"],
        ]
        expected = pd.DataFrame(contents, columns=self.columns)
        expected.reset_index(drop=True, inplace=True)
        actual = update_manifest_contents.fn(location_keys)
        self.assertTrue(expected.equals(actual))

    def test_update_manifest_contents_two_locations_populated_key_lists(self):
        s3_key_list = [
            "full/file/key_1.extension_a",
            "full/file/key_2.extension_b",
            "full/file/key_3.extension_a",
        ]
        local_key_list = [
            "full/file/key_4.extension_b",
            "full/file/key_5.extension_a",
            "full/file/key_6.extension_b",
        ]
        location_keys = {
            "s3://bucket-name": s3_key_list,
            "/local/path/": local_key_list,
        }
        contents = [
            ["key_1", "extension_a", "s3://bucket-name", "full/file/key_1.extension_a"],
            ["key_3", "extension_a", "s3://bucket-name", "full/file/key_3.extension_a"],
            ["key_5", "extension_a", "/local/path/", "full/file/key_5.extension_a"],
            ["key_2", "extension_b", "s3://bucket-name", "full/file/key_2.extension_b"],
            ["key_4", "extension_b", "/local/path/", "full/file/key_4.extension_b"],
            ["key_6", "extension_b", "/local/path/", "full/file/key_6.extension_b"],
        ]
        expected = pd.DataFrame(contents, columns=self.columns)
        expected.reset_index(drop=True, inplace=True)
        actual = update_manifest_contents.fn(location_keys)
        self.assertTrue(expected.equals(actual))

    def test_update_manifest_contents_empty_locations(self):
        expected = pd.DataFrame(columns=self.columns)
        actual = update_manifest_contents.fn({})
        self.assertTrue(expected.equals(actual))

    def test_update_manifest_contents_single_location_empty_key_list(self):
        location_keys = {"s3://bucket-name": []}
        expected = pd.DataFrame(columns=self.columns)
        actual = update_manifest_contents.fn(location_keys)
        self.assertTrue(expected.equals(actual))

    def test_update_manifest_contents_two_locations_duplicate_file(self):
        s3_key_list = ["full/file/key_1.extension_a"]
        local_key_list = ["full/file/key_1.extension_a"]
        location_keys = {
            "s3://bucket-name": s3_key_list,
            "/local/path/": local_key_list,
        }
        contents = [
            ["key_1", "extension_a", "s3://bucket-name", "full/file/key_1.extension_a"],
            ["key_1", "extension_a", "/local/path/", "full/file/key_1.extension_a"],
        ]
        expected = pd.DataFrame(contents, columns=self.columns)
        expected.reset_index(drop=True, inplace=True)
        actual = update_manifest_contents.fn(location_keys)
        self.assertTrue(expected.equals(actual))

    def test_update_manifest_contents_single_location_populated_key_list_no_directories(
        self,
    ):
        key_list = ["key_1.extension_a", "key_2.extension_b", "key_3.extension_a"]
        location_keys = {"s3://bucket-name": key_list}
        contents = [
            ["key_1", "extension_a", "s3://bucket-name", "key_1.extension_a"],
            ["key_3", "extension_a", "s3://bucket-name", "key_3.extension_a"],
            ["key_2", "extension_b", "s3://bucket-name", "key_2.extension_b"],
        ]
        expected = pd.DataFrame(contents, columns=self.columns)
        expected.reset_index(drop=True, inplace=True)
        actual = update_manifest_contents.fn(location_keys)
        self.assertTrue(expected.equals(actual))
