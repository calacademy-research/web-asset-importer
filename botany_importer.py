
from importer import Importer
import time_utils
import os
import re
import logging
from dir_tools import DirTools
from uuid import uuid4
from time_utils import get_pst_time_now_string
# I:\botany\PLANT FAMILIES
#
# I:\botany\TYPE IMAGES
# Also, there are several sub-images with the addition of '_a' or '_b'.
# Here is an example for you to check or work with to get those secondary
# images uploaded with the first:
#
# I:\botany\TYPE IMAGES\CAS_Batch13
# CAS0410512
# CAS0410512_a

class BotanyImporter(Importer):

    def __init__(self, paths, config, full_import, existing_barcodes=False, force_redacted=False,
                 skip_redacted_check=False):
        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')
        super().__init__(config, "Botany")
        # limit is for debugging
        self.skip_redacted_check = skip_redacted_check
        self.force_redacted = force_redacted
        self.botany_importer_config = config
        self.existing_barcodes = existing_barcodes
        self.full_import = full_import
        self.dir_tools = DirTools(self.build_filename_map, limit=None)
        self.paths = paths
        self.barcode_map = {}
        self.logger.debug("Botany import mode")
        self.monitoring_tools = None

        for cur_dir in self.paths:
            self.dir_tools.process_files_or_directories_recursive(cur_dir)
        self.process_loaded_files()

        if not self.full_import and self.botany_importer_config.MAILING_LIST:
            image_dict = self.image_client.imported_files
            # can add custom stats with param "value_list" if needed
            self.image_client.monitoring_tools.send_monitoring_report(subject=f"BOT_Batch: {get_pst_time_now_string()}",
                                                                      image_dict=image_dict)



    def process_loaded_files(self):
        for barcode in self.barcode_map.keys():
            filename_list = []
            for cur_filepath in self.barcode_map[barcode]:
                filename_list.append(cur_filepath)
            self.process_barcode(barcode, filename_list)


    def process_barcode(self, barcode, filepath_list):
        if barcode is None:
            self.logger.debug(f"No barcode; skipping")
            return
        self.logger.debug(f"Barcode: {barcode}")
        sql = f'''select CollectionObjectID from collectionobject where CatalogNumber={barcode};'''
        collection_object_id = self.specify_db_connection.get_one_record(sql)
        self.logger.debug(f"retrieving id for: {collection_object_id}")
        if collection_object_id is None and not self.existing_barcodes:
            self.logger.debug(f"No record found for catalog number {barcode}, creating skeleton.")
            self.create_skeleton(barcode)
            collection_object_id = self.specify_db_connection.get_one_record(sql)
            self.logger.warning(f"Skeletons temporarily disabled in botany")
            return
        #  we can have multiple filepaths per barcode in the case of barcode-a, barcode-b etc.
        # not done for modern samples, but historically this exists.
        # when removed the unittest

        filepath_list = self.clean_duplicate_basenames(filepath_list)
        filepath_list = self.clean_duplicate_image_barcodes(filepath_list)
        filepath_list = self.remove_imagedb_imported_filenames_from_list(filepath_list)


        if self.full_import and not self.existing_barcodes:
            agent_id = self.botany_importer_config.IMPORTER_AGENT_ID
        else:
            agent_id = self.botany_importer_config.AGENT_ID

        if not self.existing_barcodes or (self.existing_barcodes and collection_object_id is not None):

            self.import_to_imagedb_and_specify(filepath_list=filepath_list,
                                               collection_object_id=collection_object_id,
                                               agent_id=agent_id,
                                               force_redacted=self.force_redacted,
                                               skip_redacted_check=self.skip_redacted_check,
                                               id=barcode,
                                               )


    def build_filename_map(self, full_path):

        if not self.check_for_valid_image(full_path):
            return
        filename = os.path.basename(full_path)
        matched = re.match(self.botany_importer_config.IMAGE_SUFFIX, filename.lower())
        is_match = bool(matched)
        if not is_match:
            self.logger.debug(f"Rejected; no match: {filename}")
            return
        barcode = self.get_first_digits_from_filepath(filename)
        if barcode is None:
            self.logger.debug(f"Can't find barcode for {filename}")
            return

        self.logger.debug(f"Adding filename to mappings set: {filename}   barcode: {barcode}")
        if barcode not in self.barcode_map:
            self.barcode_map[barcode] = [full_path]
        else:
            self.barcode_map[barcode].append(full_path)


    def create_skeleton(self, barcode):
        self.logger.info(f"Creating skeleton for barcode {barcode}")
        barcode = str(barcode).zfill(9)
        cursor = self.specify_db_connection.get_cursor()
        collecting_event_guid = uuid4()
        sql = (f"""INSERT INTO collectingevent (
            TimestampCreated,
            TimestampModified,
            Version,
            GUID,
            DisciplineID
        )
        VALUES (
             %s,
             %s,
             %s,
             %s,
             %s
        );""")

        params = (
            f'{time_utils.get_pst_time_now_string()}',
            f'{time_utils.get_pst_time_now_string()}',
            0,
            f'{collecting_event_guid}',
            3
        )

        self.logger.debug(sql)
        cursor.execute(sql, params)
        self.specify_db_connection.commit()

        cursor.close()

        sql = f"select CollectingEventID from collectingevent where GUID='{collecting_event_guid}';"
        collecting_event_id = self.specify_db_connection.get_one_record(sql)

        cursor = self.specify_db_connection.get_cursor()
        sql = (f"""INSERT INTO collectionobject (
        TimestampCreated,
        TimestampModified,
        CollectingEventID,
        Version,
        CollectionMemberID,
        CatalogNumber,
        CatalogedDatePrecision,
        GUID,
        CollectionID,
        Date1Precision,
        InventoryDatePrecision    
        )
        VALUES (%s,
        %s,
        %s,
        %s,
        %s,
        %s, 
        %s,
        %s,
        %s,
        %s,
        %s
        )""")

        params = (f'{time_utils.get_pst_time_now_string()}',
        f'{time_utils.get_pst_time_now_string()}',
        collecting_event_id,
        0,
        4,
        f'{barcode}',
        1,
        f'{uuid4()}',
        4,
        1,
        1
                  )
        self.logger.debug(sql)
        cursor.execute(sql, params)
        self.specify_db_connection.commit()
        cursor.close()