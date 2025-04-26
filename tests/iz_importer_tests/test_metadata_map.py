""" 
IzImporter.__init__
├── DirectoryTree.process_files
│   └── build_filename_map
    ......
│       ├── _update_metadata_map
│       │   └── _extract_year_from_date
│       │   └── find_agent_id_from_string
│       ├── _update_casiz_filepath_map
│       └── log_file_status
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

    def test__extract_year_from_date(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        self.assertEqual(self.importer._extract_year_from_date("2020-01-01"), "2020")
        self.assertEqual(self.importer._extract_year_from_date("2023:08:15 14:35:20"), "2023")
        self.assertEqual(self.importer._extract_year_from_date("2023-01-01 12:00:00"), "2023")
        self.assertEqual(self.importer._extract_year_from_date("2024-02-01"), "2024")
        self.assertEqual(self.importer._extract_year_from_date("2012"), "2012")
        self.assertEqual(self.importer._extract_year_from_date("19890223"), None)
        self.assertEqual(self.importer._extract_year_from_date("198"), None)
        self.assertEqual(self.importer._extract_year_from_date(""), None)
        self.assertEqual(self.importer._extract_year_from_date(None), None)
    
    def test_find_agent_id_from_string(self, mock_specify_db):
        self._getImporter(mock_specify_db) 


    def test__update_metadata_map(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Import necessary constants
        from specify_constants import SpecifyConstants
        
        # Test case 1: File without key file
        self.importer.copyright = "Test Copyright"
        self.importer.title = "Test Title"
        self.importer._update_metadata_map(
            full_path="dir/file_without_key.jpg", 
            exif_metadata={'EXIF:CreateDate': "2024:01:01"}, 
            file_key=None
        )
        
        # Validate all fields in the metadata map
        metadata = self.importer.filepath_metadata_map["dir/file_without_key.jpg"]
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_DATE], "2024")
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_HOLDER], "Test Copyright")
        self.assertEqual(metadata[SpecifyConstants.ST_CREDIT], "")
        self.assertEqual(metadata[SpecifyConstants.ST_DATE_IMAGED], "2024:01:01")
        self.assertEqual(metadata[SpecifyConstants.ST_LICENSE], "")
        self.assertEqual(metadata[SpecifyConstants.ST_REMARKS], "")
        self.assertEqual(metadata[SpecifyConstants.ST_TITLE], "Test Title")
        self.assertEqual(metadata[SpecifyConstants.ST_IS_PUBLIC], False)
        self.assertEqual(metadata[SpecifyConstants.ST_SUBTYPE], "")
        self.assertEqual(metadata[SpecifyConstants.ST_TYPE], "StillImage")
        self.assertEqual(metadata[SpecifyConstants.ST_ORIG_FILENAME], "dir/file_without_key.jpg")
        self.assertEqual(metadata[SpecifyConstants.ST_CREATED_BY_AGENT_ID], "")
        self.assertEqual(metadata[SpecifyConstants.ST_METADATA_TEXT], "")
        
        # Test case 2: File with key file
        file_key = {
            'CopyrightDate': '2023-05-15',
            'CopyrightHolder': 'Key Copyright Holder',
            'Credit': 'Key Credit',
            'License': 'Key License',
            'Remarks': 'Key Remarks',
            'IsPublic': True,
            'subType': 'Key Subtype',
            'createdByAgent': 'Key Agent',
            'creator': 'Key Creator'
        }
        # set the copyright from key file
        self.importer.extract_copyright(orig_case_full_path="fake_path", exif_metadata=None, file_key=file_key)
        self.importer._update_metadata_map(
            full_path="dir/file_with_key.jpg", 
            exif_metadata={'EXIF:CreateDate': "2023"}, 
            file_key=file_key
        )
        
        # Validate all fields in the metadata map for file with key
        metadata = self.importer.filepath_metadata_map["dir/file_with_key.jpg"]
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_DATE], "2023")
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_HOLDER], "Key Copyright Holder")
        self.assertEqual(metadata[SpecifyConstants.ST_CREDIT], "Key Credit")
        self.assertEqual(metadata[SpecifyConstants.ST_DATE_IMAGED], "2023")
        self.assertEqual(metadata[SpecifyConstants.ST_LICENSE], "Key License")
        self.assertEqual(metadata[SpecifyConstants.ST_REMARKS], "Key Remarks")
        self.assertEqual(metadata[SpecifyConstants.ST_TITLE], "Test Title")
        self.assertEqual(metadata[SpecifyConstants.ST_IS_PUBLIC], True)
        self.assertEqual(metadata[SpecifyConstants.ST_SUBTYPE], "Key Subtype")
        self.assertEqual(metadata[SpecifyConstants.ST_TYPE], "StillImage")
        self.assertEqual(metadata[SpecifyConstants.ST_ORIG_FILENAME], "dir/file_with_key.jpg")
        self.assertEqual(metadata[SpecifyConstants.ST_CREATED_BY_AGENT_ID], "Key Agent")
        self.assertEqual(metadata[SpecifyConstants.ST_METADATA_TEXT], "Key Creator")
        
        # Test case 3: File with key file but missing some fields
        partial_file_key = {
            'CopyrightDate': '2023-05-15',
            'CopyrightHolder': 'Partial Key Copyright',
            # Missing other fields
        }
        
        self.importer.extract_copyright(orig_case_full_path="fake_path", exif_metadata=None, file_key=partial_file_key)
        self.importer._update_metadata_map(
            full_path="dir/file_with_partial_key.jpg", 
            exif_metadata={}, 
            file_key=partial_file_key
        )
        
        # Validate fields in the metadata map for file with partial key
        metadata = self.importer.filepath_metadata_map["dir/file_with_partial_key.jpg"]
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_DATE], "2023")
        self.assertEqual(metadata[SpecifyConstants.ST_COPYRIGHT_HOLDER], "Partial Key Copyright")
        self.assertEqual(metadata[SpecifyConstants.ST_CREDIT], "")
        self.assertEqual(metadata[SpecifyConstants.ST_DATE_IMAGED], None)
        self.assertEqual(metadata[SpecifyConstants.ST_LICENSE], "")
        self.assertEqual(metadata[SpecifyConstants.ST_REMARKS], "")
        self.assertEqual(metadata[SpecifyConstants.ST_TITLE], "Test Title")
        self.assertEqual(metadata[SpecifyConstants.ST_IS_PUBLIC], False)  # Default value when missing
        self.assertEqual(metadata[SpecifyConstants.ST_SUBTYPE], "")
        self.assertEqual(metadata[SpecifyConstants.ST_TYPE], "StillImage")
        self.assertEqual(metadata[SpecifyConstants.ST_ORIG_FILENAME], "dir/file_with_partial_key.jpg")
        self.assertEqual(metadata[SpecifyConstants.ST_CREATED_BY_AGENT_ID], "")
        self.assertEqual(metadata[SpecifyConstants.ST_METADATA_TEXT], "")
