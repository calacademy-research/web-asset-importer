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
from unittest.mock import patch

from specify_constants import SpecifyConstants

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from iz_importer_tests import TestIzImporterBase

@patch('importer.SpecifyDb')


class TestIzImporterLoadFiles(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test__get_exif_mapping(self, mock_specify_db):
        self._getImporter(mock_specify_db)
        
        # Import necessary constants
        from cas_metadata_tools import EXIFConstants
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
        # use the first 10 files from mock_data
        file_paths = list(mock_data['files'].keys())[:10]
        non_existent_file_path = 'non-existent-file.jpg'
        full_file_paths = [os.path.join(os.path.dirname(__file__), file_path) for file_path in file_paths]
        full_file_paths.append(non_existent_file_path)
        # Use the existing mock_specify_db instead of creating a new one
        with patch('importer.SpecifyDb.get_one_record') as mock_get_one_record:
            with patch('iz_importer.IzImporter.remove_specify_imported_and_id_linked_from_path') as \
                mock_remove_specify_imported_and_id_linked_from_path:
                with patch('importer.AttachmentUtils.get_attachmentid_from_filepath') as \
                    mock_get_attachmentid_from_filepath:
                    with patch('importer.Importer.import_single_file_to_image_db_and_specify') as \
                        mock_import_single_file_to_image_db_and_specify:
                        with patch('importer.ImageClient.write_exif_image_metadata') as \
                            mock_write_exif_image_metadata:
                            with patch('cas_metadata_tools.MetadataTools.write_exif_tags') as \
                                mock_write_exif_tags:
                                with patch('iz_importer.IzImporter.connect_existing_attachment_to_collection_object_id') as \
                                    mock_connect_existing_attachment_to_collection_object_id:

                                    # mock collection object id is not found
                                    mock_get_one_record.return_value = None
                                    # should return empty attachment properties map
                                    self.assertEqual(self.importer.process_casiz_number(self.importer.casiz_numbers, full_file_paths), {})
                                    mock_get_one_record.return_value = 123

                                    # mock remove_specify_imported_and_id_linked_from_path returns empty list
                                    mock_remove_specify_imported_and_id_linked_from_path.return_value = []
                                    self.assertEqual(self.importer.process_casiz_number(self.importer.casiz_numbers, full_file_paths), {})
                                    mock_remove_specify_imported_and_id_linked_from_path.return_value = full_file_paths
                                    mock_connect_existing_attachment_to_collection_object_id.return_value = True

                                    # mock get_attachmentid_from_filepath returns not None
                                    def get_attachment_id_side_effect(filepath):
                                        # Convert the full path back to the relative path used in mock_data
                                        relative_path = os.path.relpath(filepath, os.path.dirname(__file__))
                                        if relative_path in mock_data['files']:
                                            return mock_data['files'][relative_path].get('attachment_id')
                                        return None
                                    mock_get_attachmentid_from_filepath.side_effect = get_attachment_id_side_effect
                                    mock_loc = 'mock_loc'
                                    mock_import_single_file_to_image_db_and_specify.return_value = mock_loc
                                    mock_write_exif_image_metadata.return_value = True
                                    mock_write_exif_tags.return_value = True
                                    for file_path in full_file_paths:
                                        self.importer.filepath_metadata_map[file_path] = {
                                            SpecifyConstants.ST_COPYRIGHT_DATE: '2023-01-01',
                                            SpecifyConstants.ST_COPYRIGHT_HOLDER: 'Test Copyright Holder',
                                            SpecifyConstants.ST_CREDIT: 'Test Credit',
                                            SpecifyConstants.ST_DATE_IMAGED: '2023-01-01',
                                            SpecifyConstants.ST_LICENSE: 'Test License',
                                            SpecifyConstants.ST_REMARKS: 'Test Remarks',
                                            SpecifyConstants.ST_TITLE: 'Test Title',
                                            SpecifyConstants.ST_IS_PUBLIC: True,
                                            SpecifyConstants.ST_SUBTYPE: 'Test Subtype',
                                            SpecifyConstants.ST_TYPE: 'StillImage',
                                            SpecifyConstants.ST_ORIG_FILENAME: file_path,
                                            SpecifyConstants.ST_CREATED_BY_AGENT_ID: 'Test Agent ID',
                                            SpecifyConstants.ST_METADATA_TEXT: 'Test Metadata Text'
                                        }
                                    casiz_number = 12345
                                    results = self.importer.process_casiz_number(casiz_number, full_file_paths)
                                    for file_path in file_paths:
                                        full_file_path = os.path.join(os.path.dirname(__file__), file_path)
                                        if file_path == non_existent_file_path:
                                            self.assertNotIn(file_path, results)
                                        else:
                                            attachment_id = mock_data['files'][file_path].get('attachment_id')
                                            if attachment_id is not None:
                                                self.assertEqual(results[full_file_path], {'attachment_id': attachment_id})
                                            else:
                                                self.assertEqual(results[full_file_path]['attach_loc'], mock_loc)
                                                # remove the attach_loc key from the results for the next assertion
                                                results[full_file_path].pop('attach_loc')
                                                self.assertEqual(results[full_file_path], self.importer.filepath_metadata_map[full_file_path])

                                    # mock get_attachmentid_from_filepath returns not None
                                    def get_attach_loc_side_effect(cur_filepath,collection_object_id,agent_id,
                                                                    skip_redacted_check,attachment_properties_map,
                                                                    force_redacted,id):
                                        # Convert the full path back to the relative path used in mock_data
                                        relative_path = os.path.relpath(cur_filepath, os.path.dirname(__file__))
                                        if relative_path in mock_data['files']:
                                            if mock_data['files'][relative_path].get('no_attach_loc'):
                                                return None
                                        return 'mock_loc'
                                    # override the return mocked value of import_single_file_to_image_db_and_specify
                                    mock_import_single_file_to_image_db_and_specify.side_effect = get_attach_loc_side_effect
                                    results = self.importer.process_casiz_number(casiz_number, full_file_paths)

                                    # check if at least one file has no_attach_loc set to True otherwise it will be false positive
                                    num_file_has_no_attach_loc_set = 0
                                    for file_path in file_paths:
                                        full_file_path = os.path.join(os.path.dirname(__file__), file_path)
                                        if mock_data['files'][file_path].get('no_attach_loc'):
                                            num_file_has_no_attach_loc_set += 1
                                            self.assertEqual(results[full_file_path]['attach_loc'], None)
                                    self.assertEqual(num_file_has_no_attach_loc_set, 1,
                                                    "There should be one and only one file that has no_attach_loc set to True \
                                                        for this test to be valid")

if __name__ == "__main__":
    unittest.main()