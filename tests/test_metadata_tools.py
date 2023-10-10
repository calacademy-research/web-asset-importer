import unittest

import pandas as pd
import shutil
from metadata_tools import MetadataTools
from tests.test_images import test_config
class TestMetadataTools(unittest.TestCase):

    def setUp(self):
        self.md = MetadataTools()
        self.path = test_config.PATH
        shutil.copyfile("tests/test_images/test_image.jpg", "tests/test_images/image_backup.jpg")

    def test_is_file_larger_than(self):
        """testing file is larger than function"""
        self.assertFalse(self.md.is_file_larger_than(filepath=self.path, size_in_mb=100))
        self.assertTrue(self.md.is_file_larger_than(filepath=self.path, size_in_mb=1.5))
        self.assertFalse(self.md.is_file_larger_than(filepath=self.path, size_in_mb=5))

    def test_code_to_tag(self):
        """tests code to tag converter function"""
        self.assertTrue(self.md.exif_code_to_tag(271), 'Make')
        self.assertTrue(self.md.exif_code_to_tag(33432), 'Copyright')
        self.assertTrue(self.md.exif_code_to_tag(315), 'Artist')


    def test_iptc_read(self):
        """testing iptc read function"""
        iptc_dict = self.md.read_iptc_metadata(path=self.path)
        self.assertFalse(pd.isna(iptc_dict))
        self.assertNotEqual(iptc_dict, {})
        self.assertEqual(iptc_dict['contact'], [])

    def test_IPTC_attach(self):
        """testing decode_exif_data function"""
        self.md.iptc_attach_metadata(iptc_dict={'by-line': "Mateo De La Roca",
                                                'copyright notice': "@CopyrightIPTC",
                                                'caption/abstract': "An upsidedown image of a woodworking shop"},
                                     path=self.path)
        iptc_dict = self.md.read_iptc_metadata(path=self.path)
        self.assertEqual(b"@CopyrightIPTC", iptc_dict['copyright notice'])
        self.assertEqual(b"Mateo De La Roca", iptc_dict['by-line'])
        self.assertEqual(b"An upsidedown image of a woodworking shop", iptc_dict['caption/abstract'])
    def test_exif_read(self):
        """tests exif read function"""
        exif_dict = self.md.read_exif_metadata(path=self.path, convert_tags=False)
        self.assertFalse(pd.isna(exif_dict))
        self.assertNotEqual(exif_dict, {})
        self.assertEqual(exif_dict[272], 'iPhone XR')

    def test_exif_attach(self):
        """tests exif attach function"""
        self.md.iptc_attach_metadata(iptc_dict={'by-line': "Mateo De La Roca",
                                                'copyright notice': "@CopyrightIPTC",
                                                'caption/abstract': "An upsidedown image of a woodworking shop"},
                                                path=self.path)

        exif_dict = self.md.read_exif_metadata(path=self.path, convert_tags=True)
        self.assertEqual("Apple", exif_dict['Make'])
        self.assertEqual("15.2.1", exif_dict['Software'])
        self.assertEqual(3, exif_dict['Orientation'])

    def tearDown(self):
        del self.md
        shutil.copyfile("tests/test_images/image_backup.jpg", "tests/test_images/test_image.jpg")


