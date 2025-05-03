import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json
from pprint import pformat
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from iz_importer import IzImporter
@patch('importer.SpecifyDb')

class TestIzImporterBase(unittest.TestCase):

    def setUp(self):
        self.importer = None
        self.mock_data_file = os.path.join(os.path.dirname(__file__), 'iz_test_images_mock_data.json')

    def _getImporter(self, mock_specify_db, image_db_result=False):
        if self.importer:
            return

        # Create and configure ImageClient mock
        patcher = patch('importer.ImageClient')
        mock_image_client_class = patcher.start()
        self.addCleanup(patcher.stop)
        
        # Create a real dictionary for monitoring_dict
        real_monitoring_dict = {}
        
        # Configure the mock to use the real dictionary
        mock_image_client_class.monitoring_dict = real_monitoring_dict
        mock_image_client = mock_image_client_class.return_value
        mock_image_client.check_image_db_if_filename_imported.return_value = image_db_result
        mock_image_client.monitoring_dict = real_monitoring_dict  # Also set on instance

        # Create a real MonitoringTools instance with the real append_monitoring_dict
        from monitoring_tools import MonitoringTools
        mock_monitoring_tools = MagicMock(spec=MonitoringTools)
        mock_monitoring_tools.append_monitoring_dict = MonitoringTools.append_monitoring_dict
        mock_image_client.monitoring_tools = mock_monitoring_tools

        self.__initialize_importer(mock_specify_db, mock_image_client)

        return self.importer

    def __initialize_importer(self, mock_specify_db, mock_image_client):
        self.importer = IzImporter()
        if not self.importer:
            raise Exception("Failed to initialize importer")
        return self.importer
    
    def get_mock_data(self):
        with open(self.mock_data_file, 'r') as f:
            mock_data = json.load(f)
        return mock_data
    
    def update_mock_data(self, mock_data):
        new_mock_data_file = os.path.join(os.path.dirname(__file__), 'iz_test_images_mock_data_new.json')
        with open(new_mock_data_file, 'w') as f:
            json.dump(mock_data, f, indent=2, ensure_ascii=False)
