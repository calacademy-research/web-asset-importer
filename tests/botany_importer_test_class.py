from botany_importer import BotanyImporter
from importer import Importer
from dir_tools import DirTools
import logging


class AltBotanyImporter(BotanyImporter):
    def __init__(self, config, paths, full_import, existing_barcodes = False):
        self.logger = logging.getLogger('Client.BotanyImporter')
        self.botany_importer_config = config
        Importer.__init__(self, self.botany_importer_config, "Botany")
        # limit is for debugging
        self.botany_importer_config = config
        # print(self.botany_importer_config)
        self.existing_barcodes = existing_barcodes
        dir_tools = DirTools(self.build_filename_map, limit=None)
        self.barcode_map = {}
        self.logger.debug("Botany import mode")

        for cur_dir in paths:
            dir_tools.process_files_or_directories_recursive(cur_dir)
