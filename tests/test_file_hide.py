import unittest
import os
from tests.testing_tools import TestingTools
from tests.pic_importer_test_class import AltPicturaeImporter
import shutil
from get_configs import get_config

class HideFilesTest(unittest.TestCase,TestingTools):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.md5_hash = self.generate_random_md5()
    def setUp(self):
        """creating test images in test_date folder, in order to test
           file_hide and file_unhide functions, creates 3 sample test
           images with sample barcodes"""
        # create test directories

        self.config = get_config(config="Botany_PIC")


        self.expected_folder = f"../storage_01/picturae/delivery/CP1_{self.md5_hash}_BATCH_0001"

        self.create_test_images(barcode_list=[123456, 123457, 123458],
                                color='red', expected_dir=self.expected_folder)

        self.expected_image_path = f"../storage_01/picturae/delivery/CP1_{self.md5_hash}" \
                                   f"_BATCH_0001/CAS{123456}.JPG"

        # initializing
        self.test_picturae_importer = AltPicturaeImporter()

        self.test_picturae_importer.image_list = [self.expected_image_path]

    def test_file_hide(self):
        """testing whether file_hide hides files not in barcode list"""
        self.test_picturae_importer.hide_unwanted_files()
        files = os.listdir(self.expected_folder)
        self.assertTrue('CAS123456.JPG' in files)
        self.assertTrue('.hidden_CAS123457.JPG')

    def test_file_unhide(self):
        """testing whether files are correctly unhidden after running hide_unwanted_files"""
        self.test_picturae_importer.hide_unwanted_files()
        self.test_picturae_importer.unhide_files()
        files = os.listdir(self.expected_folder)
        self.assertEqual(set(files), {'CAS123456.JPG', 'CAS123457.JPG', 'CAS123458.JPG'})

    def tearDown(self):
        """deleting test directory and instance of PicturaeImporter class"""

        shutil.rmtree(os.path.dirname(self.expected_image_path))

        del self.test_picturae_importer