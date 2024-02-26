from PIC_database_updater import UpdateDbFields
import logging
from tests.sqlite_csv_utils import SqlLiteTools
from importer_config import initialize_collection_config
class AltUpdateDbFields(UpdateDbFields):
    def __init__(self, force_update=False):
        self.config = initialize_collection_config(collection="Botany_PIC")
        self.force_update = force_update
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('UpdateDbFields')
        self.sql_csv_tools = SqlLiteTools(sql_db="tests/casbotany_lite.db")
