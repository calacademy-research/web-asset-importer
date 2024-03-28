
from importer import Importer
import time_utils
from datetime import datetime
import os
import re
import logging
from dir_tools import DirTools
from uuid import uuid4
from time_utils import get_pst_time_now_string
from monitoring_tools import MonitoringTools
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
starting_time_stamp = datetime.now()

class BotanyImporter(Importer):

    def __init__(self, paths, config, full_import, existing_barcodes=False):
        self.logger = logging.getLogger('Client.BotanyImporter')
        super().__init__(config, "Botany")
        # limit is for debugging
        self.botany_importer_config = config
        # print(self.botany_importer_config)
        self.existing_barcodes = existing_barcodes
        dir_tools = DirTools(self.build_filename_map, limit=None)
        self.barcode_map = {}
        self.logger.debug("Botany import mode")

        # FILENAME = "bio_importer.bin"
        # if not os.path.exists(FILENAME):
        for cur_dir in paths:
            dir_tools.process_files_or_directories_recursive(cur_dir)

        #     outfile = open(FILENAME, 'wb')
        #     pickle.dump(self.barcode_map, outfile)
        # else:
        #     self.barcode_map = pickle.load(open(FILENAME, "rb"))

        if not full_import:
            self.monitoring_tools = MonitoringTools(config=self.botany_importer_config)

            self.monitoring_tools.create_monitoring_report()

        self.process_loaded_files()


        if not full_import:
            self.monitoring_tools.send_monitoring_report(subject=f"BOT_Batch: {get_pst_time_now_string()}",
                                                         time_stamp=starting_time_stamp)


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
        force_redacted = False
        if collection_object_id is None and not self.existing_barcodes:
            self.logger.debug(f"No record found for catalog number {barcode}, creating skeleton.")
            self.create_skeleton(barcode)
            force_redacted = True
            self.logger.warning(f"Skeletons temporarily disabled in botany")
            return
        #  we can have multiple filepaths per barcode in the case of barcode-a, barcode-b etc.
        # not done for modern samples, but historically this exists.
        # when removed the unittest
        filepath_list = self.clean_duplicate_basenames(filepath_list)
        filepath_list = self.clean_duplicate_image_barcodes(filepath_list)
        filepath_list = self.remove_imagedb_imported_filenames_from_list(filepath_list)


        if not self.existing_barcodes or (self.existing_barcodes and collection_object_id is not None):

            self.import_to_imagedb_and_specify(filepath_list,
                                               collection_object_id,
                                               self.botany_importer_config.AGENT_ID,
                                               force_redacted)



    def build_filename_map(self, full_path):
        full_path = full_path.lower()
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
            '{time_utils.get_pst_time_now_string()}',
            '{time_utils.get_pst_time_now_string()}',
            0,
            '{collecting_event_guid}',
            3
        );""")
        self.logger.debug(sql)
        cursor.execute(sql)
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
        VALUES ('{time_utils.get_pst_time_now_string()}',
        '{time_utils.get_pst_time_now_string()}',
        {collecting_event_id},
        0,
        4,
        '{barcode}', 
        1,
        '{uuid4()}',
        4,
        1,
        1
        )""")
        self.logger.debug(sql)
        cursor.execute(sql)
        self.specify_db_connection.commit()
        cursor.close()


    # def import_image(self, is_redacted, full_path):
    #     try:
    #         collection_object_id = self.get_collection_object_id(full_path)
    #         if collection_object_id is None:
    #             self.create_skeleton(full_path)
    #             print(f"Not importing {full_path}; Created skeleton", file=sys.stderr, flush=True)
    #             is_redacted = True
    #         else:
    #             if is_redacted != True:
    #                 is_redacted = self.attachment_utils.get_is_collection_object_redacted(collection_object_id)
    #
    #         url, attach_loc = self.image_client.upload_to_image_server(full_path, is_redacted, 'Botany')
    #         agent_id = 95728  # joe russack in botany
    #
    #         self.import_to_specify_database(full_path, attach_loc, url, collection_object_id, agent_id)
    #     except UploadFailureException:
    #         print(f"Upload failure to image server for file: {full_path}")
    #     except DatabaseInconsistentError:
    #         print(f"Database inconsistent for collection object id: {collection_object_id}, file: {full_path}",
    #               file=sys.stderr, flush=True)

    # def verify_and_import(self, full_path):
    #     if not os.path.isfile(full_path):
    #         self.logger.debug(f"Not a file: {full_path}")
    #     else:
    #         if self.file_regex_match:
    #             check_regex = os.path.basename(full_path)
    #             matched = re.match(self.file_regex_match, check_regex)
    #             is_match = bool(matched)
    #             self.logger.debug(f"Check regex {self.file_regex_match} on:{check_regex} in dir {full_path}: {is_match}")
    #             if not is_match:
    #                 return
    #
    #         if filetype.is_image(full_path):
    #             if self.image_client.check_image_db_if_already_imported('Botany', os.path.basename(full_path)):
    #                 print(f"Image {full_path} already imported, skipping..", file=sys.stderr, flush=True)
    #                 return
    #             if self.is_private:
    #                 is_redacted = True
    #             else:
    #                 is_redacted = False
    #             self.import_image(is_redacted, full_path)
    #
    #         else:
    #             self.logger.f"File found, but not image, skipping: {full_path}")
