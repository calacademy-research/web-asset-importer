"""test case of the PicturaeImporter class which runs a reduced init method to use in unittests"""
import logging
from tests.sqlite_csv_utils import SqlLiteTools
from importer import Importer
from picturae_importer import PicturaeImporter
import picturae_config

class TestPicturaeImporter(PicturaeImporter):
    def __init__(self, date_string, paths):
        Importer.__init__(self, db_config_class=picturae_config, collection_name="Botany")
        self.init_all_vars(date_string=date_string, paths=paths)
        self.sql_csv_tools = SqlLiteTools(sql_db="tests/casbotany_lite.db")
        self.logger = logging.getLogger("TestPicturaeImporter")