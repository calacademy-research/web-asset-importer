from attachment_utils import AttachmentUtils
import datetime
from uuid import uuid4
import os, re
from image_client import ImageClient
from db_utils import InvalidFilenameError
import collections
import filetype

import logging
import subprocess
from specify_db import SpecifyDb
import shutil
from os import listdir
from os.path import isfile, join
import traceback
import hashlib
from image_client import DuplicateImageException
from specify_constants import SpecifyConstants
import atexit


class ConvertException(Exception):
    pass


class TooSmallException(Exception):
    pass


class MissingPathException(Exception):
    pass


class Importer:

    def __init__(self, db_config_class, collection_name):
        self.db_config_class = db_config_class

        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')
        self.collection_name = collection_name
        self.specify_db_connection = SpecifyDb(db_config_class)
        self.image_client = ImageClient(config=db_config_class)
        self.attachment_utils = AttachmentUtils(self.specify_db_connection)
        self.duplicates_file = open(f'duplicates-{self.collection_name}.txt', 'w')
        self.TMP_JPG = f"./tmp_jpg_{self.image_client.generate_token(filename=str(uuid4()))}"

        self.execute_at_exit()

    def split_filepath(self, filepath):
        cur_filename = os.path.basename(filepath)
        cur_file_ext = cur_filename.split(".")[-1]
        cur_filename = cur_filename.split(".")[:-1]
        cur_filename = ".".join(cur_filename)
        return cur_filename, cur_file_ext

    def remove_tmp_jpg(self):
        """removes tmp folder after process termination"""
        if os.path.exists(self.TMP_JPG):
            self.logger.info(f"Removing ./TMP folder at {self.TMP_JPG}")
            shutil.rmtree(self.TMP_JPG)

    def execute_at_exit(self):
        """executes any custom cleanup processes
           after importer exits with exit code."""
        atexit.register(self.remove_tmp_jpg)

    @staticmethod
    def get_file_md5(filename):
        with open(filename, 'rb') as f:
            md5_hash = hashlib.md5()
            while chunk := f.read(8192):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def tiff_to_jpg(self, tiff_filepath):
        basename = os.path.basename(tiff_filepath)
        if not os.path.exists(self.TMP_JPG):
            os.mkdir(self.TMP_JPG)

        file_name_no_extention, extention = self.split_filepath(basename)
        if extention != 'tif':
            self.logger.error(f"Bad filename, can't convert {tiff_filepath}")
            raise ConvertException(f"Bad filename, can't convert {tiff_filepath}")

        jpg_dest = os.path.join(self.TMP_JPG, file_name_no_extention + ".jpg")

        proc = subprocess.Popen(['convert', '-quality', '99', tiff_filepath, jpg_dest],
                                stdout=subprocess.PIPE)

        output = proc.communicate(timeout=60)[0]
        onlyfiles = [f for f in listdir(self.TMP_JPG) if isfile(join(self.TMP_JPG, f))]
        if len(onlyfiles) == 0:
            raise ConvertException(f"No files produced from conversion")
        files_dict = {}
        for file in onlyfiles:
            files_dict[file] = os.path.getsize(os.path.join(self.TMP_JPG, file))
        sort_orders = sorted(files_dict.items(), key=lambda x: x[1], reverse=True)
        top = sort_orders[0][0]
        target = os.path.join(self.TMP_JPG, file_name_no_extention + ".jpg")
        os.rename(os.path.join(self.TMP_JPG, top), target)
        if len(onlyfiles) > 2:
            self.logger.info("multi-file case")

        return target, output

    def get_mime_type(self, filepath):
        mime_type = None
        if filepath.lower().endswith('.tif') or filepath.lower().endswith('.tiff'):
            mime_type = 'image/tiff'
        if filepath.lower().endswith('.jpg') or filepath.lower().endswith('.jpeg'):
            mime_type = 'image/jpeg'
        if filepath.lower().endswith('.gif'):
            mime_type = 'image/gif'
        if filepath.lower().endswith('.png'):
            mime_type = 'image/png'
        if filepath.lower().endswith('.pdf'):
            mime_type = 'application/pdf'
        return mime_type

    def connect_existing_attachment_to_collection_object_id(self,
                                                            attachment_id,
                                                            collection_object_id,
                                                            agent_id):
        ordinal = self.attachment_utils.get_ordinal_for_collection_object_attachment(collection_object_id)
        if ordinal is None:
            ordinal = 0
        else:
            ordinal += 1
        self.attachment_utils.create_collection_object_attachment(attachment_id,
                                                                  collection_object_id,
                                                                  ordinal,
                                                                  agent_id)

    def import_to_specify_database(self, filepath, attach_loc, url, collection_object_id, agent_id, properties):

        attachment_guid = str(uuid4())

        file_created_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))

        mime_type = self.get_mime_type(filepath)

        self.attachment_utils.create_attachment(
            attachment_location=attach_loc,
            original_filename=filepath,
            url=url,
            file_created_datetime=file_created_datetime,
            guid=str(attachment_guid),
            image_type=mime_type,
            agent_id=agent_id,
            properties=properties
        )

#        attachment_id = self.attachment_utils.get_attachment_id(attachment_guid)
        attachment_id = self.attachment_utils.get_attachment_id(str(attachment_guid))

        self.connect_existing_attachment_to_collection_object_id(attachment_id, collection_object_id, agent_id)

    def get_first_digits_from_filepath(self, filepath, field_size=9):
        basename = os.path.basename(filepath)
        ints = re.findall(r'\d+', basename)
        if len(ints) == 0:
            raise InvalidFilenameError("Can't get barcode from filename")
        int_digits = int(ints[0])
        string_digits = f"{int_digits}"
        string_digits = string_digits.zfill(field_size)
        self.logger.debug(f"extracting digits from {filepath} to get {string_digits}")
        return string_digits

    def format_filesize(self, num, suffix="B"):
        for unit in ["", "K", "M", "G", "T"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f} Yi{suffix}"

    # if the basenames are the same, it removes them.
    def clean_duplicate_basenames(self, filepath_list):
        basename_list = [os.path.basename(filepath) for filepath in filepath_list]
        duplicates = [item for item, count in collections.Counter(basename_list).items() if count > 1]

        # Write out duplicates file
        for duplicate in duplicates:
            res = [item for item in filepath_list if duplicate in item]
            self.duplicates_file.write(f'\nDuplicate: {duplicate}\n')

            for dupe_path in res:
                size = os.path.getsize(dupe_path)
                self.logger.debug(f"dupe_path: {dupe_path}")
                self.duplicates_file.write(f"\t {self.format_filesize(size)}: {dupe_path}\n")
        seen_basenames = set()
        unique_filepaths = []
        for filepath in filepath_list:
            basename = os.path.basename(filepath)
            if basename not in seen_basenames:
                seen_basenames.add(basename)
                unique_filepaths.append(filepath)
        return unique_filepaths

    @staticmethod
    def clean_duplicate_image_barcodes(filepath_list):
        file_dict = {}
        jpg_dict = {}
        for filepath in filepath_list:
            basename = os.path.basename(filepath)
            barcode, file_extension = os.path.splitext(basename)

            if barcode not in file_dict:
                file_dict[barcode] = filepath

                if file_extension.lower() in ['.jpg', '.jpeg']:
                    jpg_dict[barcode] = filepath
            else:
                # If a duplicate is encountered, prioritize .jpg files
                if file_extension.lower() in ['.jpg', '.jpeg']:
                    jpg_dict[barcode] = filepath
                    file_dict[barcode] = filepath
                elif (file_extension.lower() in ['.tif', '.tiff']) and not file_dict[barcode].lower().endswith(
                        ('.jpg', '.jpeg')):
                    file_dict[barcode] = filepath

        return list(file_dict.values())

    def convert_image_if_required(self, filepath):
        jpg_found = False
        tif_found = False
        deleteme = None
        filename, filename_ext = self.split_filepath(filepath)
        if filename_ext.lower() == "jpg" or filename_ext.lower() == "jpeg":
            jpg_found = filepath
        if filename_ext.lower() == "tif" or filename_ext.lower() == "tiff":
            tif_found = filepath
        if not jpg_found and tif_found:
            self.logger.debug(f"  Must create jpg for {filepath} from {tif_found}")

            jpg_found, output = self.tiff_to_jpg(tif_found)
            if not os.path.exists(jpg_found):
                self.logger.error(f"  Conversion failure for {tif_found}; skipping.")
                self.logger.debug(f"Imagemagik output: \n\n{output}\n\n")
                raise MissingPathException
            deleteme = jpg_found
            if not jpg_found and tif_found:
                self.logger.debug(f"  No valid files for {filepath.full_path}")
                raise InvalidFilenameError
        if os.path.getsize(jpg_found) < 1000:
            self.logger.info(f"This image is too small; {os.path.getsize(jpg_found)}, skipping.")
            return TooSmallException

        return deleteme

    def upload_filepath_to_image_database(self, filepath, redacted=False, id=None):

        deleteme = self.convert_image_if_required(filepath)

        if deleteme is not None:
            upload_me = deleteme
        else:
            upload_me = filepath

        self.logger.debug(
            f"about to import to client:- {redacted}, {upload_me}, {self.collection_name}")

        url, attach_loc = self.image_client.upload_to_image_server(upload_me,
                                                                   redacted,
                                                                   self.collection_name,
                                                                   filepath,
                                                                   id=id)
        if deleteme is not None:
            os.remove(deleteme)
        return (url, attach_loc)

    def remove_specify_imported_and_id_linked_from_path(self, filepath_list, collection_object_id):
        keep_filepaths = []
        for cur_filepath in filepath_list:
            sql = f"""
                    select at.AttachmentId
                           from attachment as at,
                           collectionobjectattachment as cat
                           where at.OrigFilename='{cur_filepath}' and 
                           cat.CollectionObjectID='{collection_object_id}'
                           and cat.AttachmentId = at.AttachmentId
                    """
            aid = self.attachment_utils.db_utils.get_one_record(sql)

            if aid is None:
                keep_filepaths.append(cur_filepath)
            else:
                logging.debug(f"Already has an attachment with attachment_id: {aid}, skipping")
        return keep_filepaths

    #
    def remove_imagedb_imported_filepaths_from_list(self, filepath_list):
        keep_filepaths = []
        for cur_filepath in filepath_list:
            if not self.image_client.check_image_db_if_filename_imported(self.collection_name,
                                                                         cur_filepath,
                                                                         exact=True):
                keep_filepaths.append(cur_filepath)
        return keep_filepaths

    def remove_imagedb_imported_filenames_from_list(self, filepath_list):
        keep_filepaths = []

        for cur_filepath in filepath_list:
            cur_filename = os.path.basename(cur_filepath)
            try:
                cur_file_base, cur_file_ext = cur_filename.split(".")
            except ValueError as e:
                print(f"Can't parse {cur_filename}, skipping.")
                continue
            if not self.image_client.check_image_db_if_filename_imported(self.collection_name,
                                                                         cur_file_base + ".jpg",
                                                                         exact=True):
                keep_filepaths.append(cur_filepath)
        return keep_filepaths

    def import_to_imagedb_and_specify(self,
                                      filepath_list,
                                      collection_object_id,
                                      agent_id,
                                      force_redacted=False,
                                      attachment_properties_map=None,
                                      skip_redacted_check=False,
                                      fill_remarks=True,
                                      id=None):
        if attachment_properties_map is None:
            attachment_properties_map = {}
        for cur_filepath in filepath_list:
            if skip_redacted_check:
                is_redacted = False
            elif force_redacted:
                is_redacted = True
            else:
                is_redacted = self.attachment_utils.get_is_collection_object_redacted(collection_object_id)

            try:
                (url, attach_loc) = self.upload_filepath_to_image_database(cur_filepath, redacted=is_redacted, id=id)

                properties = attachment_properties_map.get(cur_filepath, {})
                is_redacted_property = properties.get('is_redacted', None)
                if is_redacted_property is not None and is_redacted_property:
                    is_public = False
                else:
                    is_public = not force_redacted

                properties[SpecifyConstants.ST_IS_PUBLIC] = is_public
                if fill_remarks is False:
                    url = None
                self.import_to_specify_database(
                    filepath=cur_filepath,
                    attach_loc=attach_loc,
                    url=url,
                    collection_object_id=collection_object_id,
                    agent_id=agent_id,
                    properties=properties
                )

                return attach_loc

            except TimeoutError:
                self.logger.error(f"Timeout converting {cur_filepath}")

            except subprocess.TimeoutExpired:
                self.logger.error(f"Timeout converting {cur_filepath}")
            except DuplicateImageException:
                self.logger.error(f"Image already imported {cur_filepath}")
            except ConvertException:
                self.logger.error(f"Conversion failure for {cur_filepath}; skipping.")
            except Exception as e:
                self.logger.error(
                    f"Upload failure to image server for file: \n\t{cur_filepath}")
                self.logger.error(f"Exception: {e}")
                traceback.print_exc()

    def check_for_valid_image(self, full_path):
        # self.logger.debug(f"Ich importer verify file: {full_path}")
        if not filetype.is_image(full_path):
            logging.debug(f"Not identified as a file, looks like: {filetype.guess(full_path)}")
            if full_path.lower().endswith(".tif") or full_path.lower().endswith(".tiff"):
                print("Tiff file misidentified as not an image, overriding auto-recognition")
            else:
                return False

        filename = os.path.basename(full_path)
        if "." not in filename:
            self.logger.debug(f"Rejected; no . : {filename}")

            return False
        return True
