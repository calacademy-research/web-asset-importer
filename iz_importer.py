from datetime import datetime
from importer import Importer
from directory_tree import DirectoryTree
import os
import re
import logging
from metadata_tools import MetadataTools
from monitoring_tools import MonitoringTools
import traceback
from time_utils import get_pst_time_now_string
from get_configs import get_config
import warnings

logging.basicConfig(level=logging.WARNING)

CASIZ_FILE_LOG = "file_log.tsv"

starting_time_stamp = datetime.now()


class IzImporter(Importer):
    class item_mapping:
        def __init__(self):
            self.casiz_numbers = []

    def __init__(self, full_import):

        logging.getLogger('Client.dbutils').setLevel(logging.WARNING)
        # logging.getLogger("urllib3").setLevel(logging.WARNING)
        # logging.getLogger("mysql.connector").setLevel(logging.WARNING)

        warnings.filterwarnings("ignore", category=UserWarning)
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)
        logging.getLogger('Client.dbutils').setLevel(logging.WARNING)
        logging.getLogger('Client.importer').setLevel(logging.DEBUG)
        logging.getLogger('Client.ImageClient').setLevel(logging.DEBUG)

        self.logger.setLevel(logging.DEBUG)

        self.iz_importer_config = get_config(config="IZ")
        self.AGENT_ID = 26280
        self.log_file = open(CASIZ_FILE_LOG, "w+")
        self.item_mappings = []
        self.log_file.write(f"casiz\tfilename\tCASIZ method\tcopyright method\tcopyright\trejected\tpath on disk\n")


        self.collection_name = self.iz_importer_config.COLLECTION_NAME

        super().__init__(self.iz_importer_config, "Invertebrate Zoology")

        # dir_tools = DirTools(self.build_filename_map)

        self.casiz_filepath_map = {}
        self.path_copyright_map = {}

        self.logger.debug("IZ import mode")

        self.cur_conjunction_match = self.iz_importer_config.FILENAME_CONJUNCTION_MATCH + \
                                     self.iz_importer_config.IMAGE_SUFFIX

        self.cur_filename_match = self.iz_importer_config.FILENAME_MATCH + self.iz_importer_config.IMAGE_SUFFIX

        self.cur_casiz_match = self.iz_importer_config.CASIZ_MATCH
        self.cur_extract_casiz = self.extract_casiz
        self.directory_tree_core = DirectoryTree(self.iz_importer_config.IZ_SCAN_FOLDERS, pickle_for_debug=True)

        self.directory_tree_core.process_files(self.build_filename_map)
        # placeholder for filename now

        if not full_import:
            self.monitoring_tools = MonitoringTools(config=self.iz_importer_config)
            self.monitoring_tools.create_monitoring_report()

        print("Starting to process loaded core files...")

        self.process_loaded_files()

        if not full_import:
            self.monitoring_tools.send_monitoring_report(subject=f"IZ_BATCH:{get_pst_time_now_string()}",
                                                         time_stamp=starting_time_stamp)

    def process_loaded_files(self):

        for casiz_number in self.casiz_filepath_map.keys():
            filepaths = self.casiz_filepath_map[casiz_number]
            filepath_list = []
            #  redundant from an old cleaning operation but harmless for now
            for cur_filepath in filepaths:
                filepath_list.append(cur_filepath)

            self.process_casiz_number(casiz_number, filepath_list)

    def read_decoder_ring(self, ring_path):
        pass

    def decoder_ring_applied(self, exif, filepath):
        pass

    def needs_update(self, casiz_number, filepath):
        pass
        # self.logger.debug(f"Processing casiz_numbers: {casiz_number}")
        # sql = f"select collectionobjectid  from collectionobject where catalognumber={casiz_number}"
        # collection_object_id = self.specify_db_connection.get_one_record(sql)

    # Iterates over all the files;
    def process_casiz_number(self, casiz_number, filepath_list):
        self.logger.debug(f"Processing casiz_numbers: {casiz_number}")
        sql = f"select collectionobjectid from collectionobject where catalognumber={casiz_number}"
        collection_object_id = self.specify_db_connection.get_one_record(sql)
        if collection_object_id is None:
            print(f"No record found for casiz_number {casiz_number}, skipping.")
            return
        # remove the subset of already-seen filepaths from the filepath import list.
        # "is this in specify attached to this casiz" query
        filepath_list = self.remove_specify_imported_and_id_linked_from_path(filepath_list, collection_object_id)

        # now, check if the attachment is already in there (AND case):
        for cur_filepath in filepath_list:

            attachment_id = self.attachment_utils.get_attachmentid_from_filepath(cur_filepath)

            if attachment_id is not None:
                # if so, link attachment to this COID:
                self.connect_existing_attachment_to_collection_object_id(attachment_id,
                                                                         collection_object_id,
                                                                         self.AGENT_ID)
            else:
                # If not:

                attach_loc = self.import_to_imagedb_and_specify([cur_filepath],
                                                                collection_object_id,
                                                                self.AGENT_ID,
                                                                copyright_filepath_map=self.path_copyright_map,
                                                                force_redacted=True)

                # Copyright                       : Copyright President and Fellows of Harvard College, Peabody Museum of Archaeology and Ethnology
                # CopyrightNotice                 : Copyright President and Fellows of Harvard College, Peabody Museum of Archaeology and Ethnology
                # CopyrightFlag                   : True
                # Credit                          : Copyright President and Fellows of Harvard College, Peabody Museum of Archaeology and Ethnology
                # Rights                          : Copyright President and Fellows of Harvard College, Peabody Museum of Archaeology and Ethnology

                if cur_filepath in self.path_copyright_map:
                    copyright = self.path_copyright_map[cur_filepath]
                else:
                    copyright = "-"

                iptc_dict = {"by-line": "Picasso",
                             'Date Created': "2023-02-10",
                             "2#116": f"{copyright}1",
                             "110": f"{copyright}2"}
                # joe
                exif_dict = {
                    "33432": f"{copyright}3",
                    "XMP-dc:Rights": f"{copyright}4",
                    "XMP-dc:Credit": f"{copyright}5",
                    "IPTC:CopyrightNotice": f"{copyright}6",
                    "CopyrightFlag": "True"}

                self.image_client.write_iptc_image_metadata(iptc_dict, self.collection_name, attach_loc)
                self.image_client.write_exif_image_metadata(exif_dict, self.collection_name, attach_loc)

    def log_file_status(self,
                        id=None,
                        filename=None,
                        path=None,
                        casiznumber_method=None,
                        rejected=None,
                        copyright_method=None,
                        copyright=None,
                        conjunction=None):
        if rejected is None:
            rejected = "*"
        if casiznumber_method is None:
            casiznumber_method = "-"
        if copyright is None:
            copyright = "-"
        if id is None or rejected is True:
            id = "-"
        if conjunction:
            id = conjunction
        print(
            f"Logged: {id} copyright method: {copyright_method} copyright: \'{copyright}\' rejected:{rejected} filename: {filename} Path: {path}")
        self.log_file.write(
            f"{id}\t{filename}\t{casiznumber_method}\t{copyright_method}\t{copyright}\t{rejected}\t{path}\n")
        return

    def extract_casiz_single(self, candidate_string):
        ints = re.findall(self.iz_importer_config.CASIZ_NUMBER, candidate_string)
        if len(ints) > 0:
            return ints[0]
        return None

    def extract_casiz(self, candidate_string):
        ints = re.findall(self.iz_importer_config.CASIZ_MATCH, candidate_string)
        if len(ints) > 0:
            return ints[0][1]
        return None

    def extract_copyright_from_string(self, copyright_string):
        copyright = None

        if '©' in copyright_string:
            copyright = copyright_string.split('©')[-1]
        if 'copyright' in copyright_string:
            copyright = copyright_string.split('copyright')[-1]
        if copyright is not None:
            copyright = copyright.strip()
            copyright = re.sub(r'\s*_.*$', '', copyright)
        return copyright

    def attempt_exif_extraction(self, full_path):
        try:
            return MetadataTools(full_path)
        except Exception as e:
            print(f"Exception: {e}")
            traceback.print_exc()

    def get_casiz_from_exif(self, exif_metadata):
        if exif_metadata is None:
            return None

        if "ImageDescription" in exif_metadata.keys():
            image_description = exif_metadata['ImageDescription'].strip()
            ints = re.findall(r'\d+', image_description)
            if len(ints) == 0:
                self.logger.debug(f" Can't find any id number in the image description: {image_description}")

            else:
                if len(ints[0]) >= self.iz_importer_config.MINIMUM_ID_DIGITS:
                    casiz_number = int(ints[0])
                    self.casiz_numbers = [casiz_number]

        return None

    # old and busted
    def get_copyright_from_exif(self, exif_metadata):
        if exif_metadata is None:
            return None
        if "Copyright" in exif_metadata.keys():

            copyright = exif_metadata['Copyright'].strip()
            if copyright is not None:
                if copyright.startswith('Â'):
                    copyright = copyright[1:]
                #     Common data errors
                if len(copyright) <= 2:
                    return None
                if "\x00\x00\x00\x00\x00\x00\x00" in copyright:
                    return None
                return copyright
        return None

    def extract_copyright(self, orig_case_full_path, exif_metadata):
        orig_case_directory = os.path.dirname(orig_case_full_path)
        orig_case_filename = os.path.basename(orig_case_full_path)
        self.copyright = None

        if exif_metadata:
            copyright = self.get_copyright_from_exif(exif_metadata)
            if copyright is not None and copyright.lower() != 'copyright':
                self.copyright = copyright

                return 'exif'
        if self.attempt_directory_copyright_extraction(orig_case_directory):
            return 'directory'

        filename_copyright = self.extract_copyright_from_string(orig_case_filename)
        if filename_copyright is not None:
            copyright_method = 'filename'
            self.copyright = filename_copyright
            return 'filename'

        return None

    def attempt_directory_match(self, full_path):
        directory = os.path.dirname(full_path)

        directories = directory.split('/')

        for cur_directory in reversed(directories):

            result = re.search(self.iz_importer_config.DIRECTORY_CONJUNCTION_MATCH, cur_directory)
            if result:
                found_substring = result.groups()[0]
                self.casiz_numbers = list(set([int(num) for num in re.findall(r'\b\d+\b', found_substring)]))
                return True
            if re.search(self.iz_importer_config.DIRECTORY_MATCH, cur_directory):
                self.casiz_numbers = [self.extract_casiz(directory)]
                return True
        return False

    def attempt_filename_match(self, full_path):
        filename = os.path.basename(full_path)

        # Check for conjunction matches first
        match = re.search(self.iz_importer_config.FILENAME_CONJUNCTION_MATCH, filename)
        if match:
            # Extract all numeric groups (CASIZ numbers) from the match
            self.casiz_numbers = list(set([int(num) for num in re.findall(r'\b\d{5,12}\b', filename)]))
            print(f"Matched conjunction on {filename}. IDs: {self.casiz_numbers}")
            return True

        # Fallback to simple filename match
        if re.search(self.iz_importer_config.FILENAME_MATCH, filename):
            # Extract CASIZ number using the specific extraction method
            casiz_number = self.extract_casiz(filename)
            if casiz_number is not None:
                self.casiz_numbers = [casiz_number]
                return True

        return False

    #
    # def attempt_filename_match(self, full_path):
    #     filename = os.path.basename(full_path)
    #     cur_conjunction_match = r'cas(iz)?[#a-z _]*[_ \\-]?([0-9]{5,12}) (and|or) (cas(iz)?[#a-z _]*[_ \\-]?)?([0-9]{5,12})[a-z\\-\\(\\)0-9 ©_,.]*(\.(jpg|jpeg|tiff|tif|png))$'
    #
    #     if self.cur_conjunction_match is not None:
    #         if re.search(self.cur_conjunction_match, filename):
    #             p = re.compile(self.cur_conjunction_match)
    #             result = p.search(filename)
    #             # found_substring = result.groups()[0]
    #             # self.casiz_numbers = extracted_numbers = list(set([int(num) for num in re.findall(r'\b\d{5,}\b', ' '.join(result))]))
    #
    #             self.casiz_numbers = list(set([int(num) for num in re.findall(r'\b\d+\b', result)]))
    #             print(f"Matched conjunction on {filename}. IDs: {self.casiz_numbers}")
    #             return True
    #     if re.search(self.cur_filename_match, filename):
    #         self.casiz_numbers = [self.cur_extract_casiz(filename)]
    #
    #         return True
    #
    #     return False

    def attempt_directory_copyright_extraction(self, directory_orig_case):
        directories = directory_orig_case.split('/')

        for cur_directory in reversed(directories):
            copyright = self.extract_copyright_from_string(cur_directory)
            if copyright is not None:
                self.copyright = copyright
                return True
        return False

    def check_already_attached(self, full_path):
        attachment_id = self.attachment_utils.get_attachmentid_from_filepath(full_path)
        if attachment_id is not None:
            return True
        return False

    # def exclude_by_extension(self,full_path):
    #     extension = full_path.rsplit('.', 1)[-1]
    #     if extension in iz_importer_config.EXCLUDE_EXTENSIONS:
    #         return True
    #     else:
    #         return False

    def include_by_extension(self, filepath: str) -> bool:

        pattern = re.compile(f'^.*{self.iz_importer_config.IMAGE_SUFFIX}')

        return bool(pattern.match(filepath))

    def check_already_in_image_db(self, full_path):

        if self.image_client.check_image_db_if_filepath_imported(self.collection_name,
                                                                 full_path,
                                                                 exact=True):
            return True
        return False

    def build_filename_map(self, full_path):
        # Joe - this is a temp limiter so we can quickly debug
        if 'counter' not in globals():
            globals()['counter'] = 0
        if globals()['counter'] < 110:
            globals()['counter'] += 1
        else:
            return False

        orig_case_full_path = full_path
        full_path = full_path.lower()
        if 'crrf' in full_path:
            print("Rejecting all CRRF for now - pending mapping")
            self.log_file_status(filename=os.path.basename(full_path),
                                 path=full_path,
                                 rejected="Skipping CRRF for now")

        if not self.include_by_extension(full_path):
            print(f"Will not import, excluded extension: {full_path}")
            self.log_file_status(filename=os.path.basename(full_path),
                                 path=full_path,
                                 rejected="Forbidden extension")
            return False

        filename = full_path.split('/')[-1]

        # Check if the filename starts with .
        if filename.startswith('.'):
            print(f"Skipping all files that start with .: {full_path}")
            self.log_file_status(filename=os.path.basename(full_path),
                                 path=full_path,
                                 rejected=".filename")
            return False

        if self.check_already_attached(full_path):
            self.log_file_status(filename=os.path.basename(full_path),
                                 path=full_path,
                                 rejected="Already imported")
            return False

        # if self.check_already_in_image_db(full_path):
        #     print(f"Already in image db {orig_case_full_path}")
        #     return False
        exif_metadata = None
        exif_tools = self.attempt_exif_extraction(full_path)
        if exif_tools is not None:
            exif_metadata = exif_tools.read_exif_metadata()

        self.casiz_numbers = None
        if self.get_casiz_from_exif(exif_metadata) is not None:
            casiz_source = 'EXIF'
        else:
            if self.attempt_directory_match(full_path):
                casiz_source = 'Directory'
            else:
                if self.attempt_filename_match(full_path):
                    casiz_source = 'Filename'
                else:
                    self.log_file_status(filename=os.path.basename(full_path),
                                         path=full_path,
                                         rejected="no casiz match for exif, filename, or directory.")
                    return False

        # -------- copyright --------
        copyright_method = self.extract_copyright(orig_case_full_path, exif_metadata)

        if self.copyright:
            self.path_copyright_map[full_path] = self.copyright

        # This little horror ensures that we're all ints in the list of numbers.
        self.casiz_numbers = list(
            map(lambda x: int(x) if str(x).isdigit() else int(''.join(filter(str.isdigit, str(x)))),
                self.casiz_numbers))

        for cur_casiz_number in self.casiz_numbers:
            if cur_casiz_number not in self.casiz_filepath_map:
                self.casiz_filepath_map[cur_casiz_number] = [full_path]
            else:
                self.casiz_filepath_map[cur_casiz_number].append(full_path)

        self.log_file_status(filename=os.path.basename(orig_case_full_path),
                             path=orig_case_full_path,
                             casiznumber_method=casiz_source,
                             id=cur_casiz_number,
                             copyright_method=copyright_method,
                             copyright=self.copyright)
        return True

    def get_collectionobjectid_from_casiz_number(self, casiz_number):
        sql = f"select collectionobjectid  from collectionobject where catalognumber={casiz_number}"
        return self.specify_db_connection.get_one_record(sql)
