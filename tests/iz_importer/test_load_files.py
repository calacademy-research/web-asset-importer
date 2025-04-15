""" 
IzImporter.__init__
    ......
└── process_loaded_files
    └── process_casiz_number
        ├── remove_specify_imported_and_id_linked_from_path
        ├── import_single_file_to_image_db_and_specify
        │   └── cleanup_incomplete_import (called on exception)
        │       ├── image_client.get_internal_filename
        │       ├── attachment_utils.get_attachmentid_from_filepath
        │       ├── image_client.delete_from_image_server
        │       └── remove_file_from_database
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



if __name__ == "__main__":
    unittest.main()