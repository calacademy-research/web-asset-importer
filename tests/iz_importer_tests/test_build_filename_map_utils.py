""" 
IzImporter.__init__
├── DirectoryTree.process_files
│   └── build_filename_map
│       ├── _check_and_increment_counter
│       ├── validate_path
│       │   └── include_by_extension
│       ├── _should_skip_file
│       ├── _read_file_key
│       │   └── _find_key_file
│       ├── _is_file_already_processed
│       │   ├── check_already_attached
│       │   └── check_already_in_image_db
│       ├── _read_exif_metadata
    ......
│       ├── _update_casiz_filepath_map
│       └── log_file_status (skipped test for now)
    ......
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json

from specify_constants import SpecifyConstants

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iz_importer_tests import TestIzImporterBase

@patch('importer.SpecifyDb')


class TestIzImporterBuildFilenameMapUtils(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_validate_path(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        self.assertTrue(self.importer.validate_path("test/path/to/file.jpg"))
        self.assertFalse(self.importer.validate_path("test/path/to/file.crrf"))
        self.assertFalse(self.importer.validate_path("test/path/to/file.csv"))

    def test_include_by_extension(self, mock_specify_db):
        """Test file extension filtering"""
        self._getImporter(mock_specify_db)
        self.assertTrue(self.importer.include_by_extension("image.jpg"))
        self.assertTrue(self.importer.include_by_extension("image.jpeg"))
        self.assertTrue(self.importer.include_by_extension("image.tif"))
        self.assertTrue(self.importer.include_by_extension("image.tiff"))
        self.assertTrue(self.importer.include_by_extension("image.png"))
        self.assertTrue(self.importer.include_by_extension("image.dng"))
        
        # Invalid extensions
        self.assertFalse(self.importer.include_by_extension("image.pdf"))
        self.assertFalse(self.importer.include_by_extension("image.jpg.pdf"))
        self.assertFalse(self.importer.include_by_extension("image.txt"))
        self.assertFalse(self.importer.include_by_extension("image"))

    def test_should_skip_file(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        self.assertTrue(self.importer._should_skip_file(".jpg", "test/path/to/.jpg"))
        self.assertFalse(self.importer._should_skip_file("file.jpg", "test/path/to/file.jpg"))
        self.assertTrue(self.importer._should_skip_file(".git", "test/path/to/.git"))
        self.assertFalse(self.importer._should_skip_file("test.csv", "test/path/to/test.csv"))

    def test_find_key_file(self, mock_specify_db):
        """Test finding key files in various scenarios"""
        self._getImporter(mock_specify_db)
        
        # Setup test paths
        base_path = "/test/path"
        file_path = os.path.join(base_path, "images", "test_image.jpg")
        dir_path = os.path.dirname(file_path)
        parent_dir_path = os.path.dirname(dir_path)
        
        # Case 1: Directory exists but no CSV file
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.isdir', return_value=True), \
             patch('os.listdir', return_value=['file1.jpg', 'file2.png']):
            
            # Configure mock_exists to return True for directories, False for CSV files
            def exists_side_effect(path):
                return not path.endswith('.csv') and (path == dir_path or path == parent_dir_path)
            
            mock_exists.side_effect = exists_side_effect
            
            key_file = self.importer.find_key_file(dir_path)
            self.assertIsNone(key_file)
        
        # Case 2: CSV file exists but not named key.csv
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.isdir', return_value=True), \
             patch('os.listdir', return_value=['file1.jpg', 'metadata.csv', 'file2.png']):
            
            # Configure mock_exists to return True for directories and the non-key CSV file
            def exists_side_effect(path):
                return path == dir_path or path == parent_dir_path or path == os.path.join(dir_path, 'metadata.csv')
            
            mock_exists.side_effect = exists_side_effect
            
            key_file = self.importer.find_key_file(dir_path)
            self.assertIsNone(key_file)
        
        # Case 3: key.csv file exists under the same directory
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.isdir', return_value=True), \
             patch('os.listdir', return_value=['file1.jpg', 'key.csv', 'file2.png']), \
             patch('os.path.isfile') as mock_isfile:
            
            # mock file is key.csv
            def isfile_side_effect(path):
                return path.endswith('key.csv')
            
            mock_isfile.side_effect = isfile_side_effect
            
            # Configure mock_exists to return True for directories and key.csv
            def exists_side_effect(path):
                return path == dir_path or path == parent_dir_path or path == os.path.join(dir_path, 'key.csv')
            
            mock_exists.side_effect = exists_side_effect
            
            # Test finding key file
            key_file = self.importer.find_key_file(dir_path)
            self.assertEqual(key_file, os.path.join(dir_path, 'key.csv'))
        
        # Case 4: key.csv file exists under the parent directory
        with patch('os.path.exists') as mock_exists, \
             patch('os.path.isdir', return_value=True), \
             patch('os.listdir') as mock_listdir, \
             patch('os.path.isfile') as mock_isfile:
            
            # Configure mock_listdir to return different values for different directories
            def listdir_side_effect(path):
                if path == dir_path:
                    return ['file1.jpg', 'file2.png']
                elif path == parent_dir_path:
                    return ['images', 'key.csv', 'other_folder']
                return []
            
            mock_listdir.side_effect = listdir_side_effect
            # mock file is parent directory key.csv exists
            def isfile_side_effect(path):
                return path == os.path.join(parent_dir_path, 'key.csv')
            
            mock_isfile.side_effect = isfile_side_effect
            
            # Configure mock_exists to return True for directories and parent key.csv
            def exists_side_effect(path):
                return path == dir_path or path == parent_dir_path or path == os.path.join(parent_dir_path, 'key.csv') 
            
            mock_exists.side_effect = exists_side_effect
            
            # Test finding key file
            key_file = self.importer.find_key_file(file_path)
            self.assertEqual(key_file, os.path.join(parent_dir_path, 'key.csv'))

        # Case 5: directory is root
        self.assertIsNone(self.importer.find_key_file('/'))

    def test_read_file_key(self, mock_specify_db):
        """Test reading file keys from metadata files"""
        self._getImporter(mock_specify_db)
        
        # Load the expected metadata from the mock data file
        mock_data = self.get_mock_data()
        
        # Get the base directory for test images
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../iz_test_images/'))
        
        # Test a sample of files with metadata
        sample_files = list(mock_data['files'].keys())[:10]  # Test first 10 files
        for file_path in sample_files:
            # Construct the full path to the file
            full_path = os.path.join(base_dir, file_path)
            
            # Skip if the file doesn't exist
            if not os.path.exists(full_path):
                continue
                
            # Get the actual file key
            file_key = self.importer._read_file_key(full_path)
            
            # Get the expected metadata
            expected_metadata = mock_data['metadata'][file_path]
            
            # Verify the file key contains expected fields
            if file_key:
                self.assertIn('CopyrightHolder', file_key)
                self.assertIn('Credit', file_key)
                self.assertIn('License', file_key)
                
                # Verify the content matches what's in the mock data
                for line in expected_metadata.strip().split('\n'):
                    if line and ',' in line:
                        key, value = line.split(',', 1)
                        if key in file_key and value:
                            self.assertEqual(file_key[key], value, 
                                            f"Mismatch in {file_path} for key {key}: expected '{value}', got '{file_key[key]}'")
        
        # Test a file that doesn't exist
        nonexistent_file = "nonexistent_file.jpg"
        file_key = self.importer._read_file_key(nonexistent_file)
        self.assertIsNone(file_key)
        
        # Test a file without a key file
        # Find a file in the mock data that doesn't have a key file
        no_key_files = [file_path for file_path, file_info in mock_data['files'].items() if not file_info.get('has_key_file', True)]
        if no_key_files:
            no_key_file = os.path.join(base_dir, no_key_files[0])
            if os.path.exists(no_key_file):
                file_key = self.importer._read_file_key(no_key_file)
                self.assertIsNone(file_key)

    def test_is_file_already_processed(self, mock_specify_db):
        test_path = "test/path/to/file.jpg"
        
        # Test matrix:
        # image_db  | attachment_id | expected
        # True      | 123           | True
        # True      | None          | True
        # False     | 123           | True
        # False     | None          | False
        
        test_cases = [
            # (image_db_result, attachment_id_result, expected_result)
            (True, 123, True),
            (True, None, True),
            (False, 123, True),
            (False, None, False)
        ]

        for image_db_result, attachment_id_result, expected_result in test_cases:
            self.importer = None  # Reset importer for each test case
            self._getImporter(mock_specify_db, image_db_result=image_db_result)
            
            with patch('importer.AttachmentUtils.get_attachmentid_from_filepath', return_value=attachment_id_result):
                result = self.importer._is_file_already_processed(test_path, test_path)
                self.assertEqual(
                    result, 
                    expected_result, 
                    f"Failed when image_db={image_db_result} and attachment_id={attachment_id_result}"
                )

        # Additional test for case sensitivity
        self.importer = None
        self._getImporter(mock_specify_db, image_db_result=False)
        with patch('importer.AttachmentUtils.get_attachmentid_from_filepath', return_value=123):
            upper_path = "test/path/to/FILE.jpg"
            lower_path = upper_path.lower()
            self.assertTrue(self.importer._is_file_already_processed(lower_path, upper_path))

    def test_read_exif_metadata(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        test_file_dir = os.path.join(os.path.dirname(__file__), '../iz_test_images/')
        sample_fields = ['File:FileName', 'EXIF:Date/TimeOriginal', 'File:MIMEType']
        for dirpath, dirnames, filenames in os.walk(test_file_dir):
            for filename in filenames:
                if self.importer.validate_path(filename.lower()):
                    path = os.path.join(dirpath, filename)
                    exif_metadata = self.importer._read_exif_metadata(full_path=path)
                    # Basic check
                    assert isinstance(exif_metadata, dict), f"{path} did not return a dict"
                    #sanity check for some tags
                    for field in sample_fields:
                        assert field in exif_metadata, f"{path} does not contain {field} for {filename} in {dirpath} : {exif_metadata}"

    def test__check_and_increment_counter(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Import the iz_importer module to access its globals
        import iz_importer
        iz_importer.counter = 0
        
        self.importer._check_and_increment_counter()
        self.assertEqual(iz_importer.counter, 1)
        
        self.importer._check_and_increment_counter()
        self.assertEqual(iz_importer.counter, 2)
        
        iz_importer.counter = 100
        self.importer._check_and_increment_counter()
        self.assertEqual(iz_importer.counter, 101)

        # Reset the counter
        del iz_importer.__dict__['counter']
        self.importer._check_and_increment_counter()
        self.assertEqual(iz_importer.counter, 1)

    def test__update_casiz_filepath_map(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Test case 1: Single CASIZ number
        self.importer.casiz_numbers = [12345]
        self.importer._update_casiz_filepath_map("test/path/to/file1.jpg")
        
        # Verify the file path was added to the map
        self.assertIn(12345, self.importer.casiz_filepath_map)
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg"])
        
        # Test case 2: Multiple CASIZ numbers
        self.importer.casiz_numbers = [12345, 67890]
        self.importer._update_casiz_filepath_map("test/path/to/file2.jpg")
        
        # Verify both file paths are in the map
        self.assertIn(12345, self.importer.casiz_filepath_map)
        self.assertIn(67890, self.importer.casiz_filepath_map)
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg", "test/path/to/file2.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[67890], ["test/path/to/file2.jpg"])
        
        # Test case 3: Non-numeric CASIZ numbers (should be converted to integers)
        self.importer.casiz_numbers = ["CASIZ12345", "67890"]
        self.importer._update_casiz_filepath_map("test/path/to/file3.jpg")
        
        # Verify the non-numeric CASIZ numbers were converted to integers
        self.assertIn(12345, self.importer.casiz_filepath_map)
        self.assertIn(67890, self.importer.casiz_filepath_map)
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg", "test/path/to/file2.jpg", "test/path/to/file3.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[67890], ["test/path/to/file2.jpg", "test/path/to/file3.jpg"])
        
        # Test case 4: Empty CASIZ numbers list
        self.importer.casiz_numbers = []
        self.importer._update_casiz_filepath_map("test/path/to/file4.jpg")
        
        # Verify the map is unchanged
        self.assertEqual(len(self.importer.casiz_filepath_map), 2)  # Still only 12345 and 67890
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg", "test/path/to/file2.jpg", "test/path/to/file3.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[67890], ["test/path/to/file2.jpg", "test/path/to/file3.jpg"])
        
        # Test case 5: CASIZ numbers with non-digit characters
        self.importer.casiz_numbers = ["CASIZ-12345", "67890-ABC"]
        self.importer._update_casiz_filepath_map("test/path/to/file5.jpg")
        
        # Verify the CASIZ numbers were correctly extracted
        self.assertIn(12345, self.importer.casiz_filepath_map)
        self.assertIn(67890, self.importer.casiz_filepath_map)
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg", "test/path/to/file2.jpg", "test/path/to/file3.jpg", "test/path/to/file5.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[67890], ["test/path/to/file2.jpg", "test/path/to/file3.jpg", "test/path/to/file5.jpg"])

        # Test case 6: new CASIZ number
        self.importer.casiz_numbers = [1234567890]
        self.importer._update_casiz_filepath_map("test/path/to/file6.jpg")
        self.assertEqual(self.importer.casiz_filepath_map[12345], ["test/path/to/file1.jpg", "test/path/to/file2.jpg", "test/path/to/file3.jpg", "test/path/to/file5.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[67890], ["test/path/to/file2.jpg", "test/path/to/file3.jpg", "test/path/to/file5.jpg"])
        self.assertEqual(self.importer.casiz_filepath_map[1234567890], ["test/path/to/file6.jpg"])

    def test_build_filename_map(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        mock_data = self.get_mock_data()
        with patch('iz_importer.IzImporter.remove_file_from_database') as mock_remove_file_from_database:
            with patch('iz_importer.IzImporter._should_skip_file') as mock_should_skip_file:
                with patch('iz_importer.IzImporter._is_file_already_processed') as mock_is_file_already_processed:
                    with patch('iz_importer.IzImporter._update_metadata_map') as mock_update_metadata_map:
                        with patch('iz_importer.IzImporter._update_casiz_filepath_map') as mock_update_casiz_filepath_map:
                            with patch('iz_importer.IzImporter.log_file_status') as mock_log_file_status:
                                mock_remove_file_from_database.return_value = True
                                mock_should_skip_file.return_value = False
                                mock_is_file_already_processed.return_value = False
                                mock_update_metadata_map.return_value = True
                                mock_update_casiz_filepath_map.return_value = True
                                mock_log_file_status.return_value = True
                                for file_path, file_info in mock_data['files'].items():
                                    full_path = os.path.join(os.path.dirname(__file__), '..', file_path)
                                    result = self.importer.build_filename_map(full_path)
                                    # TODO: add assert and make it real test
                                    print(f"File {file_path} processed: {result}")

if __name__ == "__main__":
    unittest.main()