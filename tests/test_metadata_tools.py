import unittest
from metadata_tools import MetadataTools
from tests.test_images import test_config
class TestMetadataTools(unittest.TestCase):

    def setUp(self):
        self.test_image_1 = 'tests/test_images/test_image.jpg'
        self.md = MetadataTools('tests/test_images/test_image.jpg', config=test_config)
    def test_is_file_larger_than(self):
        """testing file is larger than function"""
        self.assertFalse(self.md.is_file_larger_than(filepath=self.test_image_1, size_in_mb=100))
        self.assertTrue(self.md.is_file_larger_than(filepath=self.test_image_1, size_in_mb=4))
        self.assertFalse(self.md.is_file_larger_than(filepath=self.test_image_1, size_in_mb=5))

    def test_IPTC_attach(self):
        """testing decode_exif_data function"""
        self.md.iptc_attach_metadata()
        iptc_dict = self.md.read_iptc_metadata()
        self.assertEqual(b"@CopyrightIPTC", iptc_dict['copyright notice'])
        self.assertEqual(b"Mateo De La Roca", iptc_dict['by-line'])
        self.assertEqual(b"An upsidedown image of a woodworking shop", iptc_dict['caption/abstract'])

    def test_IPTC_remove(self):
    def tearDown(self):
        del self.md


