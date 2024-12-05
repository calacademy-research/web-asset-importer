from unittest.mock import patch
import unittest
import shutil
import os
from tests.database_updater_test_class import AltUpdateDbFields
import pandas as pd
from tests.testing_tools import TestingTools
from tests.pic_importer_test_class_lite import AltPicturaeImporterlite
class TestDatabaseUpdater(unittest.TestCase, TestingTools):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.md5_hash = self.generate_random_md5()

    def setUp(self):
        """setup: creates dummy database and backup copy of sqlite database"""

        self.alt_importer_lite = AltPicturaeImporterlite(date_string=self.md5_hash)

        # creating restore point for db
        shutil.copyfile("tests/casbotany_lite.db", "tests/casbotany_backup.db")

        self.created_by_agent = 12345

        self.record_full = {"CatalogNumber": ["999999972"],
                            "verbatim_date": "April 14th 2023",
                            "start_date": "4/14/2023",
                            "end_date": "4/14/2023",
                            "collector_number": "123345",
                            "locality": "On the south slope of Mt. Tam",
                            "county": "Marin",
                            "state": "California",
                            "country": "United States",
                            "sheet_notes": "[notes]",
                            "cover_notes": "[notes]",
                            "label_data": "Random text"
                            }

        self.record_full = pd.DataFrame(self.record_full)

        for index, row in self.record_full.iterrows():
            self.alt_importer_lite.populate_fields_without_taxonomy(row=row)

        self.GeographyID = 16464

        self.alt_importer_lite.create_locality_record()

        self.alt_importer_lite.create_collecting_event()

        self.alt_importer_lite.create_collection_object()


        test_frame = {"barcode": [999999972],
                      "Lat1Text": ["38.05N"],
                      "Long1Text": ["123.05W"],
                      "Lat2Text": ["38.6N"],
                      "Long2Text": ["124.05W"],
                      "Datum": ["WGS84"],
                      "Longitude1": [-123.05],
                      "Latitude1": [38.05],
                      "Longitude2": [-124.05],
                      "Latitude2": [38.6],
                      "Township": ["48N"],
                      "Range": ["1E"],
                      "Section": ["29, NW 1/4 of NE 1/4"],
                      "UtmNorthing": ["4756577"],
                      "UtmEasting": ["503000"],
                      "UtmDatum": ["NAD83"],
                      }


        self.alt_update_db = AltUpdateDbFields(force_update=True)

        self.sql_csv_tools = self.alt_update_db.sql_csv_tools

        self.alt_update_db.update_frame = pd.DataFrame(test_frame)



    def test_add_accession_number(self):
        """tests for the update_accession function"""

        self.alt_update_db.update_accession(barcode=999999972, accession="2000000")

        # testing that a new update statement does not overwrite existing data

        self.alt_update_db.force_update = False

        self.alt_update_db.update_accession(barcode=999999972, accession="2000001")

        acc_num = self.sql_csv_tools.get_one_match(tab_name="collectionobject",
                                                   id_col="AltCatalogNumber",
                                                   key_col="CatalogNumber",
                                                   match=999999972, match_type=int)
        self.assertEqual("2000000", acc_num)


    def test_update_herb_code(self):
        """tests for the update_herbarium_code function"""

        self.alt_update_db.update_herbarium_code(barcode=999999972, herb_code="CAS")

        # testing that a new update statement does not overwrite existing data

        self.alt_update_db.force_update = False

        self.alt_update_db.update_herbarium_code(barcode=999999972, herb_code="JS")

        herb_code = self.sql_csv_tools.get_one_match(tab_name="collectionobject",
                                                     id_col="Modifier",
                                                     key_col="CatalogNumber",
                                                     match=999999972, match_type=int)
        self.assertEqual("CAS", herb_code)


    def test_update_locality_string(self):
        """tests for the update_locality_string function"""

        self.alt_update_db.update_locality_string(barcode=999999972, loc_string="On the south slope of Mt. Tam")

        # testing that a new update statement does not overwrite existing data

        self.alt_update_db.force_update = False

        self.alt_update_db.update_herbarium_code(barcode=999999972, herb_code="On the north shore of lake Bon Tempe")

        loc_string = self.sql_csv_tools.get_one_match(tab_name="locality",
                                                      id_col="LocalityName",
                                                      key_col="GUID",
                                                      match=self.alt_importer_lite.locality_guid
                                                      )
        self.assertEqual("On the south slope of Mt. Tam", loc_string)



    def test_update_habitat(self):
        """test for the update_habitat function"""

        self.alt_update_db.update_habitat(barcode=999999972, habitat_string="growing on rocky serpentine soil")

        # testing that a new update statement does not overwrite existing data

        self.alt_update_db.force_update = False

        self.alt_update_db.update_herbarium_code(barcode=999999972, herb_code="in marshy soil, with associated "
                                                                              "spec. Equisetum telmateia var. braunii")

        hab_string = self.sql_csv_tools.get_one_match(tab_name="collectingevent",
                                                      id_col="Remarks",
                                                      key_col="GUID",
                                                      match=self.alt_importer_lite.collecting_event_guid
                                                      )

        self.assertEqual("growing on rocky serpentine soil", hab_string)



    def test_update_elevation(self):
        """tests for the update_elevation function"""

        self.alt_update_db.update_elevation(barcode=999999972, max_elev=40, min_elev=30, elev_unit="m")

        # testing that a new update statement does not overwrite existing data

        self.alt_update_db.force_update = False

        self.alt_update_db.update_elevation(barcode=999999972, max_elev=55, min_elev=25, elev_unit="ft.")

        col_list = ["MaxElevation", "MinElevation", "OriginalElevationUnit"]

        val_list = [40, 30, "m"]

        for index, column in enumerate(col_list):

            elev_val = self.sql_csv_tools.get_one_match(tab_name="locality", id_col=column, key_col="LocalityID",
                                                        match=self.alt_importer_lite.locality_id)

            self.assertEqual(val_list[index], elev_val)



    def test_update_coords(self):
        """tests for the update_coords function"""


        up_list = self.alt_update_db.make_update_list(check_list=['Longitude1', 'Latitude1',
                                                                  'Longitude2', 'Latitude2',
                                                                  'Lat1Text', 'Long1Text',
                                                                  'Lat2Text', 'Long2Text', 'Datum'])

        for index, row in self.alt_update_db.update_frame.iterrows():

            val_list = row[up_list]

            self.alt_update_db.update_coords(barcode=999999972, colname_list=up_list,
                                             val_list=val_list)


            for index, column in enumerate(up_list):

                coord_val = self.sql_csv_tools.get_one_match(tab_name="locality",
                                                             id_col=column,
                                                             key_col="LocalityID",
                                                             match=self.alt_importer_lite.locality_id
                                                             )

                self.assertEqual(coord_val, val_list[column])


    def test_update_localitydetail(self):
        """test for the update_locality_det function """

        for index, row in self.alt_update_db.update_frame.iterrows():

            self.alt_update_db.create_locality_detail_tab(row=row)

            locality_det_id = self.sql_csv_tools.get_one_match(tab_name='localitydetail',
                                                               id_col='LocalityDetailID',
                                                               key_col='LocalityID',
                                                               match=self.alt_importer_lite.locality_id,
                                                               match_type=int)
            self.assertFalse(pd.isna(locality_det_id))

            up_list = self.alt_update_db.make_update_list(check_list=['Township', 'RangeDesc',
                                                                      'Section', 'UtmEasting',
                                                                      'UtmNorthing', 'UtmDatum'])
            val_list = row[up_list]

            for index, column in enumerate(up_list):

                det_val = self.sql_csv_tools.get_one_match(tab_name="localitydetail",
                                                           id_col=column,
                                                           key_col="LocalityDetailID",
                                                           match=locality_det_id
                                                          )

                self.assertEqual(str(det_val), str(val_list[column]))



    def tearDown(self):
        del self.alt_update_db
        del self.alt_importer_lite
        shutil.copyfile("tests/casbotany_backup.db", "tests/casbotany_lite.db")
        os.remove("tests/casbotany_backup.db")

