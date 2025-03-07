"""test case of the PicturaeImporter class which runs a reduced init method to use in unittests"""
import logging
import pandas as pd
from sql_csv_utils import SqlCsvTools
from importer import Importer
from picturae_importer import PicturaeImporter
from tests.testing_tools import TestingTools
from get_configs import get_config
class AltPicturaeImporter(PicturaeImporter):
    def __init__(self):
        self.picturae_config = get_config(config="Botany_PIC")
        Importer.__init__(self, db_config_class=self.picturae_config, collection_name="Botany")
        self.picdb_config = get_config(config="picbatch")
        self.testing_tools = TestingTools()
        self.process_csv_files()
        self.record_full = pd.DataFrame()
        self.init_all_vars()
        self.sql_csv_tools = SqlCsvTools(config=self.picturae_config)
        self.logger = logging.getLogger("AltPicturaeImporter")


    def process_csv_files(self):

        self.csv_folder = self.picturae_config.CSV_FOLDER
        md5 = self.testing_tools.generate_random_md5()
        self.file_path = f"{md5}/PIC_record_99999999_99999999.csv"