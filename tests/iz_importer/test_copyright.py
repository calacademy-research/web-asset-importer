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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from iz_importer import IzImporter
from test_base import TestIzImporterBase

@patch('importer.SpecifyDb')


class TestIzImporterCopyright(TestIzImporterBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    


if __name__ == "__main__":
    unittest.main()