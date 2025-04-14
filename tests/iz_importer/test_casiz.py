""" 
IzImporter.__init__
├── DirectoryTree.process_files
│   └── build_filename_map
    ......
│       ├── get_casiz_ids
│       │   ├── attempt_filename_match
│       │   │   └── extract_casiz_from_string
│       │   │       ├── extract_exact_casiz_match
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
from unittest.mock import MagicMock, patch
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from iz_importer import IzImporter
from test_base import TestIzImporterBase

@patch('importer.SpecifyDb')


class TestIzImporterCasiz(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_extract_casiz_single(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Valid cases - numbers with 5-10 digits
        test_cases = [
            ("12345", 12345),               # Minimum digits (5)
            ("123456", 123456),             # 6 digits
            ("1234567", 1234567),           # 7 digits
            ("12345678", 12345678),         # 8 digits
            ("123456789", 123456789),       # 9 digits
            ("1234567890", 1234567890),     # Maximum digits (10)
            ("123456789012", 1234567890),   # more than 10 digits
            ("12345678901_CASIZ.jpg", 1234567890),  # more than 10 digits in filename
            ("CASIZ 12345", 12345),         # With CASIZ prefix
            ("abc123456def", 123456),       # Number embedded in text
            ("12345.jpg", 12345),           # Number in filename
            ("1234abc1234565", 1234565),    # Number with letters in middle
            ("path/to/123456.jpg", 123456), # Number in path
            ("IMG_1234567_CASIZ.jpg", 1234567), # Complex filename
            ("test-12345", 12345),          # Number with dash
            ("©12345", 12345),              #latin-1 encoding
        ]
        
        for input_str, expected in test_cases:
            self.assertEqual(
                self.importer.extract_casiz_single(input_str), 
                expected,
                f"Failed to extract {expected} from '{input_str}'"
            )
        
        # Invalid cases
        invalid_cases = [
            "1234",          # Too few digits (< 5)
            "abcdef",        # No numbers
            "",              # Empty string
            "12.34",         # Decimal number
            "123-456",       # Hyphenated number
            "1234a6789",    # Number interrupted by letter
            "12.345.678",    # Numbers separated by dots
        ]
        
        for input_str in invalid_cases:
            self.assertIsNone(
                self.importer.extract_casiz_single(input_str),
                f"Should return None for invalid input '{input_str}'"
            )

    def test_extract_exact_casiz_match(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        # test cases for CASIZ_NUMBER_EXACT
        CASIZ_NUMBER_EXACT_test_cases = [
            # Valid matches
            ("casiz12345", "12345"),
            ("casiz_sample_67890", "67890"),
            ("test casiz_image-54321", "54321"),
            ("casiz_image-543", "543"),
            ("image casiz_54321", "54321"),

            ("abc_casiz_label# 88888", "88888"),
            ("casiz path test 99999", "99999"),

            # Mixed case prefix
            ("CaSiZ_image_12345", "12345"),

            # 'casiz' present but no digits
            ("casiz-only", None),
            ("casiz_image", None),

            ("casiz path\\_99999", None),

            ("casiz/image_dir_00001", None),
            ("casiz©-77777", None),
            # Edge: Multiple digits in string, ensure it picks only the one after CAS
            ("image123 casiz_something-44444 file999", "44444"),
        ]

        # test cases for CASIZ_MATCH
        CASIZ_MATCH_test_cases = [
            ("cas 123", "cas 123"),
            ("casiz test 123", "casiz test 123"),
            ("cas_abc_123456def", "cas_abc_123456"),
            ("cas_abc_1234def", "cas_abc_1234"),
            ("cas_x-1234def", "cas_x-1234"),
            ("cas1234", "cas1234"),
            ("ca 125", None),
            ("cas 1", None),
            ("12", None),
            ("cas 123456789012test", "cas 1234567890"),
            ("image123 _something-4444 file999", None),

        ]
        
        for input_str, expected in CASIZ_NUMBER_EXACT_test_cases:
            result = self.importer.extract_exact_casiz_match(input_str)
            if expected is None:
                self.assertIsNone(result, f"Expected None for input '{input_str}', but got {result}")
            else:
                self.assertIsNotNone(result, f"Expected match for input '{input_str}', but got None")
                self.assertEqual(
                    result.group(2),
                    expected,
                    f"Failed to extract {expected} from '{input_str}'"
                )
        for input_str, expected in CASIZ_MATCH_test_cases:
            result = self.importer.extract_exact_casiz_match(input_str)
            if expected is None:
                self.assertIsNone(result, f"Expected None for input '{input_str}', but got {result}")
            else:
                self.assertIsNotNone(result, f"Expected match for input '{input_str}', but got None")
                self.assertEqual(result.group(0), expected, f"Failed to extract {expected} from '{input_str}'")

    def test_extract_exact_casiz_match_file_name(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Get all image files from test directory
        mock_data_file = os.path.join(os.path.dirname(__file__), 'iz_test_images_mock_data.json')
        with open(mock_data_file, 'r') as f:
            mock_data = json.load(f)
        for file_path, casiz_info in mock_data['casiz_number_cases'].items():
            result = self.importer.extract_exact_casiz_match(file_path)
            if casiz_info is not None:
                self.assertEqual(result.group(), casiz_info)
            else:
                self.assertIsNone(result)

    def test_get_casiz_from_exif(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Get all image files from test directory
        test_file_dir = os.path.join(os.path.dirname(__file__), '..')
        mock_data_file = os.path.join(os.path.dirname(__file__), 'iz_test_images_mock_data.json')
        with open(mock_data_file, 'r') as f:
            mock_data = json.load(f)

        for file_path, casiz_numbers in mock_data['casiz_number_cases_exif'].items():
            file_path = os.path.join(test_file_dir, file_path)
            exif_metadata = self.importer._read_exif_metadata(file_path)
            result = self.importer.get_casiz_from_exif(exif_metadata)
            if result is not None:
                self.assertEqual(result, casiz_numbers)
            else:
                self.assertIsNone(result)
    
    def test_attempt_directory_match(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        # Get all image files from test directory
        test_file_dir = os.path.join(os.path.dirname(__file__), '..')
        mock_data_file = os.path.join(os.path.dirname(__file__), 'iz_test_images_mock_data.json')
        with open(mock_data_file, 'r') as f:
            mock_data = json.load(f)
        for file, casiz_numbers in mock_data['directory_matches'].items():
            directory = os.path.join(test_file_dir, file)
            result = self.importer.attempt_directory_match(directory)

            if result == True:
                self.assertEqual(self.importer.casiz_numbers, casiz_numbers, f"Expected {casiz_numbers} for {directory}")
            else:
                self.assertEqual(self.importer.casiz_numbers, [], f"Expected empty list for {directory}")
            self.importer.casiz_numbers = []

if __name__ == "__main__":
    unittest.main()