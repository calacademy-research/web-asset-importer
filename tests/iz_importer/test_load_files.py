""" 
IzImporter.__init__
    ......
└── process_loaded_files
    └── process_casiz_number
        ├──X remove_specify_imported_and_id_linked_from_path (Importer function)
        ├──X import_single_file_to_image_db_and_specify (Importer function)
        │   └──X cleanup_incomplete_import (called on exception)
        │       ├──X image_client.get_internal_filename
        │       ├──X attachment_utils.get_attachmentid_from_filepath
        │       ├──X image_client.delete_from_image_server
        │       └──X remove_file_from_database (Importer function)
        └── _get_exif_mapping 
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


class TestIzImporterLoadFiles(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test__get_exif_mapping(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Import necessary constants
        from metadata_tools.EXIF_constants import EXIFConstants
        from specify_constants import SpecifyConstants
        
        # Create a sample attachment properties map with all possible keys
        attachment_properties_map = {
            SpecifyConstants.ST_METADATA_TEXT: "Test Metadata Text",
            SpecifyConstants.ST_DATE_IMAGED: "2023-01-01",
            SpecifyConstants.ST_TITLE: "Test Title",
            SpecifyConstants.ST_COPYRIGHT_HOLDER: "Test Copyright Holder",
            SpecifyConstants.ST_CREDIT: "Test Credit",
            SpecifyConstants.ST_LICENSE: "Test License",
            SpecifyConstants.ST_FILE_CREATED_DATE: "2023-01-02",
            # Some keys intentionally left out to test None handling
        }
        
        # Call the method
        exif_mapping = self.importer._get_exif_mapping(attachment_properties_map)
        
        # Verify that the mapping is correct
        self.assertEqual(exif_mapping[EXIFConstants.EXIF_ARTIST], "Test Metadata Text")
        self.assertEqual(exif_mapping[EXIFConstants.EXIF_CREATE_DATE], "2023-01-01")
        self.assertEqual(exif_mapping[EXIFConstants.EXIF_IMAGE_DESCRIPTION], "Test Title")
        self.assertEqual(exif_mapping[EXIFConstants.IPTC_COPYRIGHT_NOTICE], "Test Copyright Holder")
        self.assertEqual(exif_mapping[EXIFConstants.IPTC_BY_LINE], "Test Metadata Text")
        self.assertEqual(exif_mapping[EXIFConstants.IPTC_CAPTION_ABSTRACT], "Test Title")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_CREDIT], "Test Credit")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_CREATOR], "Test Metadata Text")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_USAGE], "Test License")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_USAGE_TERMS], "Test License")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_CREATE_DATE], "2023-01-02")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_TITLE], "Test Title")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_DATE_CREATED], "2023-01-01")
        self.assertEqual(exif_mapping[EXIFConstants.EXIF_COPYRIGHT], "Test Copyright Holder")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_RIGHTS], "Test Copyright Holder")
        self.assertEqual(exif_mapping[EXIFConstants.IFD0_COPYRIGHT], "Test Copyright Holder")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_RIGHTS_USAGE_TERMS], "Test License")
        self.assertEqual(exif_mapping[EXIFConstants.XMP_PLUS_IMAGE_SUPPLIER_NAME], "Test Credit")
        self.assertEqual(exif_mapping[EXIFConstants.PHOTOSHOP_CREDIT], "Test Credit")
        
        # Verify that keys with None values are removed
        self.assertNotIn(EXIFConstants.IPTC_CREDIT, exif_mapping)
        
        # Test with an empty attachment properties map
        empty_mapping = self.importer._get_exif_mapping({})
        
        # Verify that all keys with None values are removed
        for key in empty_mapping:
            self.assertIsNotNone(empty_mapping[key], f"Key {key} should not be in the mapping")
    
    def test_process_casiz_number(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        mock_data = self.get_mock_data()
        with patch('iz_importer.IzImporter.specify_db_connection.get_one_record') as mock_get_one_record:
            with patch('iz_importer.IzImporter.remove_specify_imported_and_id_linked_from_path') as \
                mock_remove_specify_imported_and_id_linked_from_path:
                with patch('iz_importer.IzImporter.attachment_utils.get_attachmentid_from_filepath') as \
                    mock_get_attachmentid_from_filepath:
                    with patch('iz_importer.IzImporter.image_client.import_single_file_to_image_db_and_specify') as \
                        mock_import_single_file_to_image_db_and_specify:
                        with patch('iz_importer.IzImporter.image_client.write_exif_image_metadata') as \
                            mock_write_exif_image_metadata:
                            with patch('MetadataTools.write_exif_tags') as \
                                mock_write_exif_tags:
                                
                                mock_get_one_record.return_value = 123
                                mock_remove_specify_imported_and_id_linked_from_path.return_value = mock_data['filepath_list']
                                mock_get_attachmentid_from_filepath.return_value = None
                                mock_import_single_file_to_image_db_and_specify.return_value = None
                                mock_write_exif_image_metadata.return_value = None
                                mock_write_exif_tags.return_value = None
                                self.importer.process_casiz_number(mock_data['casiz_number'], mock_data['filepath_list'])


if __name__ == "__main__":
    unittest.main()