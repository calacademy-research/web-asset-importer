"""test case of the PicturaeImporter class which runs a reduced init method to use in unittests"""
import logging
from sql_csv_utils import SqlCsvTools
from importer import Importer
from picturae_importer import PicturaeImporter
from gen_import_utils import read_json_config
class AltPicturaeImporter(PicturaeImporter):
    def __init__(self, date_string, paths):
        self.picturae_config = read_json_config(collection="Botany_PIC")
        Importer.__init__(self, db_config_class=self.picturae_config, collection_name="Botany")
        self.picdb_config = read_json_config(collection="picbatch")
        self.init_all_vars(date_string=date_string, paths=paths)
        self.sql_csv_tools = SqlCsvTools(config=self.picturae_config)
        self.logger = logging.getLogger("AltPicturaeImporter")
