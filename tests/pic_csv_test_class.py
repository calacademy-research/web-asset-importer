"""test case of the CsvCreatePicturae class which runs a reduced init method to use in unittests"""
from importer import Importer
from picturae_csv_create import CsvCreatePicturae
from get_configs import get_config
class AltCsvCreatePicturae(CsvCreatePicturae):
    def __init__(self, date_string):
        self.picturae_config = get_config(config="Botany_PIC")
        Importer.__init__(self, db_config_class=self.picturae_config, collection_name="Botany")
        self.paths = ["test/path/folder"]
        self.picdb_config = get_config(config="picbatch")
        self.init_all_vars(date_string)

