"""test case of the CsvCreatePicturae class which runs a reduced init method to use in unittests"""
from importer import Importer
from picturae_csv_create import CsvCreatePicturae
from gen_import_utils import read_json_config
class AltCsvCreatePicturae(CsvCreatePicturae):
    def __init__(self, date_string):
        self.picturae_config = read_json_config(collection="Botany_PIC")
        Importer.__init__(self, db_config_class=self.picturae_config, collection_name="Botany")
        self.picdb_config = read_json_config(collection="picbatch")
        self.init_all_vars(date_string)

