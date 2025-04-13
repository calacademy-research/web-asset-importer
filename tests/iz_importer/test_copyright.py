""" 
IzImporter.__init__
├── _configure_logging 
├── _initialize_log_file
├── DirectoryTree.process_files
│   └── build_filename_map
        ......
│       ├── extract_copyright
│       │   ├── get_copyright_from_exif
│       │   ├── attempt_directory_copyright_extraction
│       │   │   └── extract_copyright_from_string
│       │   └── extract_copyright_from_string
│       ├── _update_metadata_map
│       ├── _update_casiz_filepath_map
│       └── log_file_status
    ......
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from iz_importer import IzImporter


@patch('importer.SpecifyDb')


class TestIzImporter(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.importer = None

    def _getImporter(self, mock_specify_db, image_db_result=False):
        if self.importer:
            return

        # Create and configure ImageClient mock
        patcher = patch('importer.ImageClient')
        mock_image_client_class = patcher.start()
        self.addCleanup(patcher.stop)
        mock_image_client = mock_image_client_class.return_value
        mock_image_client.check_image_db_if_filename_imported.return_value = image_db_result

        self.__initialize_importer(mock_specify_db, mock_image_client)
        return self.importer

    def __initialize_importer(self, mock_specify_db, mock_image_client):
        self.importer = IzImporter()
        if not self.importer:
            raise Exception("Failed to initialize importer")
        return self.importer


if __name__ == "__main__":
    unittest.main()