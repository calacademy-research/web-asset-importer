"""tests for the taxon_concat and taxon_check_real function (using tnrs)"""
import os
import pandas as pd
import unittest
from tests.pic_csv_test_class import TestCsvCreatePicturae
from tests.testing_tools import TestingTools

os.chdir("./image_client")
class ConcatTaxonTests(unittest.TestCase, TestingTools):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.md5_hash = self.generate_random_md5()
    def setUp(self):
        """creates fake taxon columns in
           dummy dataset to test out taxon_concat string output"""
        self.test_csv_create_picturae = TestCsvCreatePicturae(date_string=self.md5_hash)

        # jose Gonzalez is a real agent,
        # to make sure true matches are not added to list.
        data = {'CatalogNumber': [12345, 12346, 12347, 12348],
                'Genus': ['x Serapicamptis', 'Castilleja', 'Rafflesia', 'Castilloja'],
                'Species': [pd.NA, 'miniata', 'arnoldi', 'Moniata'],
                'Rank 1': [pd.NA, 'subsp.', 'var.', pd.NA],
                'Epithet 1': [pd.NA, 'dixonii', 'atjehensis', pd.NA],
                'Rank 2': [pd.NA, 'var.', pd.NA, pd.NA],
                'Epithet 2': [pd.NA, 'fake x cool', pd.NA, pd.NA],
                'Hybrid': [True, True, False, False]
                }

        self.test_csv_create_picturae.record_full = pd.DataFrame(data)

    def test_taxon_concat_string(self):
        """tests whether correct full taxon name string is returned from taxon_concat"""
        temp_taxon_list = []
        for index, row in self.test_csv_create_picturae.record_full.iterrows():
            self.test_csv_create_picturae.taxon_concat(row)
            temp_taxon_list.extend(self.test_csv_create_picturae.taxon_concat(row))
        self.assertEqual(temp_taxon_list[0], 'x Serapicamptis')
        self.assertEqual(temp_taxon_list[3], 'x Serapicamptis')
        self.assertEqual((temp_taxon_list[5]), 'Castilleja miniata')
        self.assertEqual((temp_taxon_list[6]), 'Castilleja miniata subsp. dixonii')
        self.assertEqual((temp_taxon_list[7]), 'Castilleja miniata subsp. dixonii')
        self.assertEqual((temp_taxon_list[8]), 'fake x cool')
        self.assertEqual((temp_taxon_list[9]), 'Castilleja miniata subsp. dixonii var. fake x cool')
        self.assertEqual((temp_taxon_list[10]), 'Rafflesia arnoldi')
        self.assertEqual((temp_taxon_list[12]), 'Rafflesia arnoldi var. atjehensis')
        self.assertEqual(len(temp_taxon_list), 20)



    def test_check_taxon_real(self):
        """test the tnrs name resolution service in the check_taxon_real function"""

        self.test_csv_create_picturae.record_full[['gen_spec', 'fullname',
                                            'first_intra',
                                            'taxname', 'hybrid_base']] = \
            self.test_csv_create_picturae.record_full.apply(self.test_csv_create_picturae.taxon_concat, axis=1, result_type='expand')

        self.test_csv_create_picturae.taxon_check_real()
        # assert statements
        self.assertEqual(len(self.test_csv_create_picturae.record_full.columns), 15)

        # 2 rows left as the genus level hybrid Serapicamptis and the mispelled "Castilloja" should fail

        self.assertEqual(len(self.test_csv_create_picturae.record_full), 2)

        self.assertTrue('name_matched' in self.test_csv_create_picturae.record_full.columns,
                        "does not contain name_matched")



    def tearDown(self):
        """deleting instance of PicturaeImporter"""
        del self.test_csv_create_picturae
