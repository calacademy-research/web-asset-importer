from PIC_database_updater import UpdatePICFields
import logging
from tests.sqlite_csv_utils import SqlLiteTools
from get_configs import get_config
class AltUpdateDbFields(UpdatePICFields):
    def __init__(self, force_update=False):
        self.config = get_config(config="Botany_PIC")
        self.force_update = force_update
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('UpdateDbFields')
        self.sql_csv_tools = SqlLiteTools(sql_db="tests/casbotany_lite.db")
