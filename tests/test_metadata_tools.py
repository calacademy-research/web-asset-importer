import unittest

import pandas as pd

from metadata_tools import MetadataTools
from tests.test_images import test_config
class TestMetadataTools(unittest.TestCase):

    def setUp(self):
        self.md = MetadataTools()
        self.iptc_categories = test_config.IPTC_Categories
        self.path = test_config.PATH
        self.exif_decoder_ring = test_config.EXIF_DECODER_RING

    def test_is_file_larger_than(self):
        """testing file is larger than function"""
        self.assertFalse(self.md.is_file_larger_than(filepath=self.path, size_in_mb=100))
        self.assertTrue(self.md.is_file_larger_than(filepath=self.path, size_in_mb=1.5))
        self.assertFalse(self.md.is_file_larger_than(filepath=self.path, size_in_mb=5))


    def test_iptc_read(self):
        """testing iptc read function"""
        iptc_dict = self.md.read_iptc_metadata(path=self.path)
        self.assertFalse(pd.isna(iptc_dict))
        self.assertNotEqual(iptc_dict, {})
        self.assertEqual(iptc_dict['contact'], [])

    def test_IPTC_attach(self):
        """testing decode_exif_data function"""
        self.md.iptc_attach_metadata(iptc_dict=self.iptc_categories, path=self.path)
        iptc_dict = self.md.read_iptc_metadata(path=self.path)
        self.assertEqual(b"@CopyrightIPTC", iptc_dict['copyright notice'])
        self.assertEqual(b"Mateo De La Roca", iptc_dict['by-line'])
        self.assertEqual(b"An upsidedown image of a woodworking shop", iptc_dict['caption/abstract'])
    def test_exif_read(self):
        exif_dict = self.md.read_exif_metadata(path=self.path)
        self.assertFalse(pd.isna(exif_dict))
        self.assertNotEqual(exif_dict, {})
        self.assertEqual(exif_dict[271], 'Apple')

    def test_exif_attach(self):
        self.md.iptc_attach_metadata(iptc_dict=self.iptc_categories, path=self.path)
        exif_dict = self.md.read_exif_metadata(path=self.path)
        self.assertEqual("copyright notice", exif_dict[33432])
        self.assertEqual("Author", exif_dict[315])
        self.assertEqual("Description", exif_dict[270])

    def tearDown(self):
        del self.md


