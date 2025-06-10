""" 
IzImporter.__init__
├── DirectoryTree.process_files
│   └── build_filename_map
    ......
│       ├── get_casiz_ids
│       │   ├── attempt_filename_match
│       │   │   └── extract_casiz_from_string
│       │   │       └── extract_casiz_single
│       │   ├── get_casiz_from_exif
│       │   │   └── extract_casiz_from_string
│       │   └── attempt_directory_match
│       │       └── extract_casiz_single
    ......
"""

import os
import sys
import unittest
from unittest.mock import patch

from cas_metadata_tools.metadata_tools_main import MetadataTools

import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iz_importer_tests import TestIzImporterBase

@patch('importer.SpecifyDb')

class TestIzImporterCasiz(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_extract_casiz_from_string(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Get all image files from test directory
        test_cases = [
            ("12345 and 67890", [12345, 67890]),
            ("99999 or 88888 or 77777", [99999, 88888, 77777]),
            ("casiz_123456 VS 2233", [123456]),
            ("1234234", [1234234]),
            ("1234567890", [1234567890]),
            ("123456789012", [123456789012]),
            ("12345678901_CASIZ.jpg", [12345678901]),
            ("casiz123456def", [123456]),
            ("CASIZ 12345", [12345]),
            ("12345.jpg", [12345]),
            ("1234cas1234565", [1234565]),
            ("some/random_file", None),
            ("archive/casiz-123456 and 78901 and cas#654321.png", [123456, 78901, 654321]),
            ("12345 and 12345", [12345]),
            ("cas1234", [1234]),
            ("cas_x-1234def", None),
            ("1234abc1234565", None),
            ("abc123456def", None),
            ("ca 125", None),
            ("cas 1", None),
            ("12", None)
        ]
        for input_str, expected in test_cases:
            result = self.importer.extract_casiz_from_string(input_str)
            if expected is None:
                self.assertFalse(result, f"Expected None for input '{input_str}', but got {result}")
                self.assertEqual(self.importer.casiz_numbers, [], f"Expected empty list for {input_str}")
            else:
                self.assertTrue(result, f"Expected {expected} for input '{input_str}', but got {result}")
                self.importer.casiz_numbers.sort()
                expected.sort()
                self.assertEqual(self.importer.casiz_numbers, expected, f"Failed to extract {expected} from '{input_str}'")
                self.importer.casiz_numbers = []

    def test_attempt_filename_match(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Get all image files from test directory
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            if file_info.get('skip_test'):
                continue
            result = self.importer.attempt_filename_match(os.path.basename(file_path))
            if file_info['casiz']['from_filename'] is not None:
                self.assertTrue(result, f"Expected match {file_info['casiz']['from_filename']} for {file_path}")
                self.assertEqual(self.importer.casiz_numbers, file_info['casiz']['from_filename'], \
                                 f"Expected {file_info['casiz']['from_filename']} for {file_path}")
            else:
                self.assertFalse(result, f"Expected False for {file_path}")
            self.importer.casiz_numbers = []



    def test_get_casiz_from_exif(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Get all image files from test directory
        test_file_dir = os.path.dirname(__file__)
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            if file_info.get('skip_test'):
                continue
            file_path = os.path.join(test_file_dir, file_path)
            exif_metadata = self.importer._read_exif_metadata(file_path)
            result = self.importer.get_casiz_from_exif(exif_metadata)
            if file_info['casiz']['from_exif'] is not None:
                self.assertEqual(result, file_info['casiz']['from_exif'], \
                                 f"Expected {file_info['casiz']['from_exif']} for {file_path}, {exif_metadata}")
            else:
                self.assertIsNone(result, f"Expected None for {file_path}, {exif_metadata}")

    def test_clear_exif_fields(self, mock_specify_db):
        import shutil, tempfile, pathlib
        self._getImporter(mock_specify_db)

        test_file_dir = os.path.dirname(__file__)
        mock_data = self.get_mock_data()
        sample_rel_path = next(iter(mock_data['files'].keys()))
        src_path = os.path.join(test_file_dir, sample_rel_path)

        # work on a temporary copy, remove afterwards
        tmp_fd, tmp_path = tempfile.mkstemp(
            suffix=pathlib.Path(sample_rel_path).suffix
        )
        os.close(tmp_fd)
        shutil.copy2(src_path, tmp_path)


        # full_path = os.path.join(test_file_dir, sample_rel_path)

        exif_tool = MetadataTools(tmp_path)

        original_tags = exif_tool.read_exif_tags()
        original_values = {
            f: original_tags.get(f)
            for f in self.importer.iz_importer_config.CLEAR_EXIF_FIELDS
        }

        self.addCleanup(
            lambda: exif_tool.write_exif_tags(original_values, overwrite_blank=True)
        )

        try:
            self.importer._clear_exif_fields(tmp_path)
            cleared_tags = exif_tool.read_exif_tags()
            for field in self.importer.iz_importer_config.CLEAR_EXIF_FIELDS:
                self.assertFalse(cleared_tags.get(field), f"{field} was not cleared")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_attempt_directory_match(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        # Get all image files from test directory
        test_file_dir = os.path.dirname(__file__)
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            if file_info.get('skip_test'):
                continue
            directory = os.path.join(test_file_dir, file_path)
            result = self.importer.attempt_directory_match(directory)

            if result == True:
                self.assertEqual(self.importer.casiz_numbers, file_info['casiz']['from_directory'], \
                                 f"Expected {file_info['casiz']['from_directory']} for {directory}")
            else:
                self.assertEqual(self.importer.casiz_numbers, [], f"Expected empty list for {directory}")
            self.importer.casiz_numbers = []

    def test_get_casiz_ids(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        # Get all image files from test directory
        test_file_dir = os.path.dirname(__file__)
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            if file_info.get('skip_test'):
                continue
            full_path = os.path.join(test_file_dir, file_path)
            exif_metadata = self.importer._read_exif_metadata(full_path)
            casiz_from = self.importer.get_casiz_ids(file_path, exif_metadata)
            self.assertEqual(casiz_from, file_info['casiz']['from'], \
                             f"Expected {file_info['casiz']['from']} for {file_path}")

        # test cases for directory match (we currently no such sample file in iz_test_images directory)
        file_path = 'root/casiz 12345/mytest.jpg'
        exif_metadata = {}
        casiz_from = self.importer.get_casiz_ids(file_path, exif_metadata)
        self.assertEqual(casiz_from, 'Directory', \
                             f"Expected Directory for {file_path}")

        # test cases for no casiz match from all sources
        file_path = 'nocasiz/nocasiz.jpg'
        exif_metadata = {}
        casiz_from = self.importer.get_casiz_ids(file_path, exif_metadata)
        self.assertIsNone(casiz_from, f"Expected None for {file_path}")

if __name__ == "__main__":
    unittest.main()