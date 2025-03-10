import pandas as pd
from tests.pic_importer_test_class import AltPicturaeImporter
import unittest
from tests.testing_tools import TestingTools

class TestAgentList(unittest.TestCase, TestingTools):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    def setUp(self):
        """creating instance of PicturaeImporter, +
           creating dummy dataset of real and fake names"""


        self.test_picturae_importer = AltPicturaeImporter()

        # jose Gonzalez is a real agent,
        # to make sure true matches are not added to list.

        data = {'agent_id1': ['', ''],
                'collector_first_name1': ['Bob', 'Joe'],
                'collector_last_name1': ['Fakeson jr.', 'DiMaggio'],
                'collector_middle_name1': ['J', 'S'],
                'agent_id2': ['', ''],
                'collector_first_name2': ['Enrique', pd.NA],
                'collector_last_name2': ['de la fake', pd.NA],
                'collector_middle_name2': ['X', pd.NA],
                'agent_id3': ['', ''],
                'collector_first_name3': ['Jose', pd.NA],
                'collector_last_name3': ['Gonzalez', pd.NA],
                'collector_middle_name3': ['Isabel', pd.NA],
                'sheet_notes': ['notes', 'notes']
                }

        self.test_picturae_importer.record_full = pd.DataFrame(data)

        self.test_picturae_importer.collector_list = []

    def test_agent_list(self):
        """makes sure the correct list of dictionaries is produced of collectors,
           where new agents are included, and old agents are excluded from new_collector_list"""
        # jose gonzalez is an existing agent , who is not meant to be in the temp_agent_list
        temp_agent_list = []
        for index, row in self.test_picturae_importer.record_full.iterrows():
            self.test_picturae_importer.create_agent_list(row)
            temp_agent_list.extend(self.test_picturae_importer.new_collector_list)

        first_dict = temp_agent_list[0]
        second_dict = temp_agent_list[1]
        third_dict = temp_agent_list[2]

        # array
        collectors = [[first_dict['collector_first_name'], 'Bob'], [first_dict['collector_last_name'], 'Fakeson'],
                      [first_dict['collector_title'], 'jr.'], [second_dict['collector_first_name'], 'Enrique'],
                      [second_dict['collector_middle_initial'], 'X'],
                      [third_dict['collector_first_name'], 'Joe'], [third_dict['collector_last_name'], 'DiMaggio'],
                      [len(temp_agent_list), 3]
                      ]

        for comparison in collectors:
            self.assertEqual(comparison[0], comparison[1])

    def tearDown(self):
        """deleting instance of self.PicturaeImporter"""
        del self.test_picturae_importer
