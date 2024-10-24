import os

import pandas as pd
from sql_csv_utils import SqlCsvTools
import logging
from importer import Importer
from gen_import_utils import cont_prompter
from string_utils import remove_non_numerics
from get_configs import get_config

class RemovePartialAttachments(Importer):
    def __init__(self, config):
        super().__init__(db_config_class=config, collection_name=config.COLLECTION_NAME)
        self.config = config
        self.collection_name = config.COLLECTION_NAME
        self.logger = logging.getLogger("RemovePartialAttachments")
        self.logger.setLevel(logging.DEBUG)
        self.sql_csv_tools = SqlCsvTools(config=self.config)
        self.attachment_tab = pd.read_csv('casbotany_attachment.csv', low_memory=False)

        self.image_tab = pd.read_csv('image_db.csv', low_memory=False)

        self.internal_list = []

        # self.remove_missing_collection_ob_attach()
        #
        # self.remove_unattached_image_paths()

        # self.remove_attachements_no_record()

        self.remove_list_of_internal_filepaths(internal_path_list=self.internal_list)

        # self.check_success()

    def remove_missing_collection_ob_attach(self):
        """remove_missing_collection_ob_attach: removes all attachments and image_db paths
         which are not connected to a collection object attachment"""
        sql = '''SELECT attachment.AttachmentLocation, attachment.AttachmentID, attachment.origFilename, attachment.GUID
                 FROM attachment
                 LEFT JOIN collectionobjectattachment ON attachment.AttachmentID = collectionobjectattachment.AttachmentID
                 WHERE collectionobjectattachment.AttachmentID IS NULL;'''

        data = self.specify_db_connection.get_records(query=sql)

        no_coa_df = pd.DataFrame(data, columns=['AttachmentLocation', 'AttachmentID', 'origFilename', 'GUID'])

        self.logger.warning(f"{len(no_coa_df)} records with no collectionobjectattachment to be deleted")

        cont_prompter()

        attachment_id_list = list(no_coa_df['AttachmentID'])

        origfile_list = list(no_coa_df['origFilename'])

        attachment_id_list = [str(item) for item in attachment_id_list]

        column_list = ', '.join(attachment_id_list)


        file_path_count = []

        test_orig_filelist = []

        for index, filepath in enumerate(data):
            test_orig_filelist.append(origfile_list[index])
            file_path_count.append(filepath[0])
            self.logger.warning(f"removing internal file path: {filepath[0]}")
            self.image_client.delete_from_image_server(attach_loc=filepath[0], collection=self.config.COLLECTION_NAME)


        sql = f'''DELETE FROM attachment WHERE AttachmentID IN ({column_list});'''

        self.sql_csv_tools.insert_table_record(sql=sql)



    def remove_unattached_image_paths(self, remove_assoc=True):
        """removes image path without attachment from db, and deletes all associated records with barcode,
            from attachments and collectionobjectattachment tables, to allow for re-ingest."""

        self.attachment_tab.rename(columns={'AttachmentLocation': 'internal_filename'}, inplace=True)

        combine_tab = pd.merge(self.attachment_tab, self.image_tab, on="internal_filename", how='outer', indicator=True)

        combine_tab = combine_tab[combine_tab['_merge'] == 'right_only']

        combine_tab = combine_tab[['internal_filename', 'original_filename']]

        combine_tab['original_filename'] = combine_tab['original_filename'].apply(remove_non_numerics)

        # combine_tab.to_csv("/admin/web-asset-importer/temp_utility_scripts/combine_tab.csv")
        print(f"number of filepaths to be removed from image db {len(combine_tab)}")
        #
        cont_prompter()

        barcode_test_count = []
        filepath_test_count = []
        attachment_id_test_count = []

        for index, row in combine_tab.iterrows():
            self.logger.info(f"for barcode: {row['original_filename']}")

            internal_filename = row['internal_filename']
            barcode = row['original_filename']

            barcode_test_count.append(barcode)

            sql = f"""SELECT AttachmentLocation FROM attachment WHERE origFilename like '%{barcode}%';"""

            internal_filepaths = self.specify_db_connection.get_records(sql)

            self.logger.info(f"removing unattached filepath: {row['internal_filename']}")
            self.image_client.delete_from_image_server(attach_loc=internal_filename, collection=self.collection_name)
            filepath_test_count.append(internal_filename)
            if remove_assoc is True and internal_filepaths:
                for filepath in internal_filepaths:
                    self.logger.info(f"removing associated filepath: {filepath[0]}")
                    filepath_test_count.append(filepath[0])
                    self.image_client.delete_from_image_server(attach_loc=filepath[0], collection=self.collection_name)

            sql = f"""SELECT AttachmentID FROM attachment WHERE origFilename like '%{barcode}%';"""

            attachment_ids = self.specify_db_connection.get_records(query=sql)

            # this will work well only after the first function which
            # eliminates attachments without collectionobjectattachments
            if attachment_ids:
                for attachment_id in attachment_ids:
                    self.logger.info(f"removing collectionobjectattachment and attachment: {attachment_id[0]}")
                    attachment_id_test_count.append(attachment_id[0])
                    sql = f"""DELETE FROM collectionobjectattachment WHERE AttachmentID = {attachment_id[0]};"""
                    self.sql_csv_tools.insert_table_record(sql=sql)
                    sql = f"""DELETE FROM attachment WHERE AttachmentID = {attachment_id[0]};"""
                    self.sql_csv_tools.insert_table_record(sql=sql)

        self.logger.warning(f"Number of barcodes processed: {len(barcode_test_count)}")
        self.logger.warning(f"Number filepaths deleted from image_server: {len(filepath_test_count)}")
        self.logger.warning(f"Number of attachments/collectionobjectattachments deleted: {len(attachment_id_test_count)}")


    def remove_list_of_internal_filepaths(self, internal_path_list: list):
        """use with caution, simple function used to remove every internal filepath in a given list from image server"""
        print(f"preparing to remove {len(internal_path_list)} paths from image server")
        # cont_prompter()
        for internal_filepath in internal_path_list:
            self.logger.info(f"removing unattached filepath: {internal_filepath}")
            try:
                self.image_client.delete_from_image_server(attach_loc=internal_filepath, collection=self.collection_name)
            except Exception as e:
                self.logger.info(e)
                continue

    def remove_attachements_no_record(self):
        """remove_attachments_no_record: used to remove attachment DB records with no image DB record."""
        self.attachment_tab.rename(columns={'AttachmentLocation': 'internal_filename'}, inplace=True)

        combine_tab = pd.merge(self.attachment_tab, self.image_tab, on="internal_filename", how='outer', indicator=True)


        combine_tab = combine_tab[combine_tab['_merge'] == 'left_only']

        combine_tab = combine_tab[['internal_filename', 'original_filename']]

        print(f"number of attachment records to remove {len(combine_tab)}")



        cont_prompter()

        for index, row in combine_tab.iterrows():
            internal_filename = row['internal_filename']
            sql = f"""SELECT AttachmentID FROM attachment WHERE AttachmentLocation = '{internal_filename}';"""

            attachment_id = self.specify_db_connection.get_one_record(sql=sql)

            sql = f"""DELETE FROM collectionobjectattachment WHERE AttachmentID = {attachment_id};"""


            self.sql_csv_tools.insert_table_record(sql=sql)

            sql = f"""DELETE FROM attachment WHERE AttachmentLocation = '{internal_filename}';"""

            self.sql_csv_tools.insert_table_record(sql=sql)

        print(f"number of filepaths to be removed from image db {len(combine_tab)}")




    def check_success(self):
        self.attachment_tab.rename(columns={'AttachmentLocation': 'internal_filename'}, inplace=True)

        combine_tab = pd.merge(self.attachment_tab, self.image_tab, on="internal_filename", how='outer', indicator=True)

        combine_tab = combine_tab[combine_tab['_merge'] == 'right_only']


config = get_config(config="Botany")
rpa = RemovePartialAttachments(config=config)

