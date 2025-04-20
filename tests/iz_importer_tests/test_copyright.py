""" 
IzImporter.__init__
├── DirectoryTree.process_files
│   └── build_filename_map
        ......
│       ├── extract_copyright
│       │   ├── get_copyright_from_exif
│       │   ├── attempt_directory_copyright_extraction
│       │   │   └── extract_copyright_from_string
│       │   └── extract_copyright_from_string
    ......
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iz_importer_tests import TestIzImporterBase

@patch('importer.SpecifyDb')


class TestIzImporterCopyright(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_extract_copyright_from_string(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Test cases with © symbol
        self.assertEqual(self.importer.extract_copyright_from_string("Image © John Doe"), "John Doe")
        self.assertEqual(self.importer.extract_copyright_from_string("© John Doe"), "John Doe")
        self.assertEqual(self.importer.extract_copyright_from_string("© John Doe_extra"), "John Doe")
        
        # Test cases with 'copyright' word
        self.assertEqual(self.importer.extract_copyright_from_string("Image copyright John Smith"), "John Smith")
        self.assertEqual(self.importer.extract_copyright_from_string("copyright John Smith"), "John Smith")
        self.assertEqual(self.importer.extract_copyright_from_string("copyright John Smith_123"), "John Smith")
        
        # Test cases with leading/trailing whitespace
        self.assertEqual(self.importer.extract_copyright_from_string("©    Jane Doe    "), "Jane Doe")
        self.assertEqual(self.importer.extract_copyright_from_string("copyright    Jane Doe    "), "Jane Doe")
        
        # Test cases with both © symbol and 'copyright' word
        self.assertEqual(self.importer.extract_copyright_from_string("© copyright John Doe"), "John Doe")
        self.assertEqual(self.importer.extract_copyright_from_string("copyright © John Doe"), "John Doe")
        self.assertEqual(self.importer.extract_copyright_from_string("Image © copyright John Doe_extra"), "John Doe")
        
        # Test cases with multiple underscores
        self.assertEqual(self.importer.extract_copyright_from_string("© John_Doe_2023"), "John")
        self.assertEqual(self.importer.extract_copyright_from_string("copyright John_Doe_Photography"), "John")
        
        # Test cases with special characters and formatting
        self.assertEqual(self.importer.extract_copyright_from_string("©\tJohn Doe\n"), "John Doe")
        
        # Test cases that should return None
        self.assertIsNone(self.importer.extract_copyright_from_string("Bla Bla"))
        self.assertIsNone(self.importer.extract_copyright_from_string(""))
        self.assertIsNone(self.importer.extract_copyright_from_string(None))

    def test_attempt_directory_copyright_extraction(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            result = self.importer.attempt_directory_copyright_extraction(os.path.dirname(file_path))
            self.assertEqual(self.importer.copyright, file_info['copyright']['from_directory'], \
                             f"Expected {file_info['copyright']['from_directory']} for {file_path}")
            self.importer.copyright = None

    def test_get_copyright_from_exif(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        mock_data = self.get_mock_data()
        for file_path, file_info in mock_data['files'].items():
            file_path = os.path.join(os.path.dirname(__file__), file_path)
            exif_metadata = self.importer._read_exif_metadata(file_path)
            result = self.importer.get_copyright_from_exif(exif_metadata)
            self.assertEqual(result, file_info['copyright']['from_exif'], \
                             f"Expected {file_info['copyright']['from_exif']} for {file_path}")
    
    def test_extract_copyright(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Load mock data
        mock_data = self.get_mock_data()
        # Test each file in the mock data
        for file_path, file_info in mock_data['files'].items():
            
            # Mock file_key with CopyrightHolder
            file_key = file_info['metadata']
            # Mock exif metadata: this will not be used because get_copyright_from_exif is patched below
            fake_exif_metadata = {
                'EXIF:Copyright': file_info.get('casiz_from_exif')
            }
            # Mock the methods
            with patch.object(self.importer, 'get_copyright_from_exif') as mock_get_copyright_from_exif:
                with patch.object(self.importer, 'attempt_directory_copyright_extraction') as mock_attempt_directory:
                    with patch.object(self.importer, 'extract_copyright_from_string') as mock_extract_copyright_from_string:
                    # Set up the mock returns
                        mock_get_copyright_from_exif.return_value = file_info.get('copyright').get('from_exif')
                        mock_attempt_directory.return_value = file_info.get('copyright').get('from_directory') is not None
                        mock_extract_copyright_from_string.return_value = file_info.get('copyright').get('from_filename')
                    
                        # Call the method
                        result = self.importer.extract_copyright(file_path, fake_exif_metadata, file_key)
                        self.assertEqual(result, file_info.get('copyright_source'), \
                                         f"Expected {file_info.get('copyright_source')} for {file_path}")
                        
                        # Reset the copyright for the next test
                        self.importer.copyright = None


if __name__ == "__main__":
    unittest.main()