"""test case of the CsvCreatePicturae class which runs a reduced init method to use in unittests"""
from importer import Importer
from picturae_csv_create import CsvCreatePicturae
import picturae_config

class TestCsvCreatePicturae(CsvCreatePicturae):
    def __init__(self, date_string):
        Importer.__init__(self, db_config_class=picturae_config, collection_name= "Botany")
        self.init_all_vars(date_string)

