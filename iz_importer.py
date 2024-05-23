import sys
from datetime import datetime
from importer import Importer
from directory_tree import DirectoryTree
import os
import re
import logging
from metadata_tools import MetadataTools
from monitoring_tools import MonitoringTools
from constants import *
from time_utils import get_pst_time_now_string
from get_configs import get_config
import warnings
import csv


logging.basicConfig(level=logging.WARNING)

CASIZ_FILE_LOG = "file_log.tsv"

starting_time_stamp = datetime.now()


# specify attachment has copyright date and copyright holder. Create based on copyright
# extraction.




def _update_metadata_map(self, full_path, exif_metadata, orig_case_full_path, file_key):

    # Extract the 4-digit year from EXIF:CreateDate if available
    exif_create_date = exif_metadata.get('EXIF:CreateDate', '')
    exif_create_year = None
    if exif_create_date:
        exif_match = re.search(r'\b\d{4}\b', exif_create_date)
        if exif_match:
            exif_create_year = exif_match.group(0)

    #





class IzImporter(Importer):
    class item_mapping:
        def __init__(self):
            self.casiz_numbers = []

    def __init__(self, full_import):
        warnings.filterwarnings("ignore", category=UserWarning)
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)
        logging.getLogger('Client.dbutils').setLevel(logging.WARNING)
        logging.getLogger('Client.importer').setLevel(logging.DEBUG)
        logging.getLogger('Client.ImageClient').setLevel(logging.DEBUG)
        logging.getLogger('Client.IzImporter').setLevel(logging.DEBUG)

        self.iz_importer_config = get_config(config="IZ")
        self.AGENT_ID = 26280
        self.log_file = open(CASIZ_FILE_LOG, "w+")
        self.item_mappings = []
        self.log_file.write(f"casiz\tfilename\tCASIZ method\tcopyright method\tcopyright\trejected\tpath on disk\n")
        self.filepath_metadata_map={}
        self.collection_name = self.iz_importer_config.COLLECTION_NAME

        super().__init__(self.iz_importer_config, "Invertebrate Zoology")

        # dir_tools = DirTools(self.build_filename_map)

        self.casiz_filepath_map = {}
        self.filepathpath_metadata_map = {}

        self.logger.debug("IZ import mode")

        self.cur_conjunction_match = self.iz_importer_config.FILENAME_CONJUNCTION_MATCH + \
                                     self.iz_importer_config.IMAGE_SUFFIX

        self.cur_filename_match = self.iz_importer_config.FILENAME_MATCH + self.iz_importer_config.IMAGE_SUFFIX

        self.cur_casiz_match = self.iz_importer_config.CASIZ_MATCH
        self.cur_extract_casiz = self.extract_casiz
        self.directory_tree_core = DirectoryTree(self.iz_importer_config.IZ_SCAN_FOLDERS, pickle_for_debug=False)

        self.directory_tree_core.process_files(self.build_filename_map)
        # placeholder for filename now

        if not full_import:
            self.monitoring_tools = MonitoringTools(config=self.iz_importer_config,
                                                    report_path=self.iz_importer_config.REPORT_PATH)
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

    def generate_attachment_properties(self, filepath):
        print("Joe - get iptc and file pate date and things for here. ")
        attachment_properties_map = {}
        metadata = self.filepathpath_metadata_map[filepath]
        copyright = metadata["copyright"]
        exif_metadata = metadata["exif_metadata"]
        print("Joe stop here - process exif data into attachment properties")
        sys.exit(1)
        attachment_properties_map["copyright_holder"] = copyright
        return attachment_properties_map

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

        # now, check if the attachment is already in there (AND/OR case):
        for cur_filepath in filepath_list:

            attachment_id = self.attachment_utils.get_attachmentid_from_filepath(cur_filepath)

            if attachment_id is not None:
                # if so, link attachment to this COID:
                self.connect_existing_attachment_to_collection_object_id(attachment_id,
                                                                         collection_object_id,
                                                                         self.AGENT_ID)
            else:
                # If not:
                attachment_properties_map = self.generate_attachment_properties(cur_filepath)

                attach_loc = self.import_to_imagedb_and_specify([cur_filepath],
                                                                collection_object_id,
                                                                self.AGENT_ID,
                                                                attachment_properties_map=attachment_properties_map,
                                                                force_redacted=True)

                if attach_loc is None:
                    self.logger.error(f"Failed to upload image, aborting upload for {cur_filepath}")
                    return

                exif_dict = {
                    "EXIF:Artist": "CAS1",
                    "EXIF:Copyright": "CAS2",
                    "EXIF:CreateDate": "2024:01:01 00:00:00",
                    "EXIF:ImageDescription": "joe",
                    "IPTC:Credit": "CAS3",
                    "IPTC:CopyrightNotice": "CAS4",
                    "IPTC:By-line": "CAS5",
                    "IPTC:By-lineTitle": "CAS6",
                    "IPTC:Caption-Abstract": "CAS7",
                    "IPTC:Keywords": "CAS8",
                    "Photoshop:CopyrightFlag": "True",
                    "XMP:Rights": "CAS10",
                    "XMP:Credit": "CAS12",
                    "XMP:Creator": "CAS13",
                    "XMP:Usage": "CAS14",
                    "XMP:UsageTerms": "CAS15",
                    "XMP:CreatorWorkURL": "CAS16",
                    "XMP:CreateDate": "2024:01:01 00:00:00",
                    "XMP:Title": "CAS18",
                    "XMP:Label": "CAS19",
                    "XMP:CreatorAddress": "CAS20 52 Music Concourse Drive",
                    "XMP:CreatorCity": "CAS21 San Francisco",
                    "XMP:CreatorCountry": "CAS22 San Francisco",
                    "XMP:CreatorRegion": "CAS23 San Francisco",
                    "XMP:CreatorPostalCode": "CAS24 San Francisco",
                    "XMP:DateCreated": "2024:01:01 00:00:00",
                    "XMP-dc:Description": "CAS25"
                }

                exif_keys = [
                    "EXIF:Artist",
                    "EXIF:Copyright",
                    "EXIF:CreateDate",
                    "EXIF:ImageDescription",
                    "EXIF:Title",
                    "IPTC:Credit",
                    "IPTC:CopyrightNotice",
                    "IPTC:By-line",
                    "IPTC:By-lineTitle",
                    "IPTC:Caption-Abstract",
                    "IPTC:Keywords",
                    "Photoshop:CopyrightFlag",
                    "XMP:Rights",
                    "XMP:Credit",
                    "XMP:Creator",
                    "XMP:Usage",
                    "XMP:UsageTerms",
                    "XMP:CreatorWorkURL",
                    "XMP:CreateDate",
                    "XMP:Title",
                    "XMP:DateCreated",
                    "XMP-dc:Description",
                    "XMP-dc:Subject",
                    "XMP-lr:HierarchicalSubject",
                    "EXIF:IFD0:ImageDescription"
                ]

                self.image_client.write_exif_image_metadata(exif_dict, self.collection_name, attach_loc)
                sys.exit(1)  # joe

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

    def get_casiz_from_exif(self, exif_metadata):
        print("Joe stop here - not using these new data yet")
        sys.exit(1)
        # EXIF:Title
        # IPTC:keywords
        # XMP-dc:Subject
        # XMP-lr:HierarchicalSubject
        # IPTC:Caption-Abstract
        # XMP-dc:Description
        # EXIF:IFD0:ImageDescription
        # file path

        if exif_metadata is None:
            return None

        # This code is probably wrong; it should use the same code we're using to extract
        # ids (wtih and/or) from filename and/or directory - maybe not, that has
        # pretty specific regular expressions. TBD.
        image_description_key = next((key for key in exif_metadata if "ImageDescription" in key), None)
        if image_description_key:
            image_description = exif_metadata[image_description_key].strip()
            ints = re.findall(r'\d+', image_description)
            if not ints:
                self.logger.debug(f"Can't find any ID number in the image description: {image_description}")
                return None
            elif len(ints[0]) >= self.iz_importer_config.MINIMUM_ID_DIGITS:
                casiz_number = int(ints[0])
                self.casiz_numbers = [casiz_number]
                return casiz_number


    def get_copyright_from_exif(self, exif_metadata):
        if exif_metadata is None:
            return None
        copyright_keys = ["EXIF:Copyright",
                          "IPTC:CopyrightNotice"]
        if any(key.lower() in (k.lower() for k in exif_metadata.keys()) for key in copyright_keys):

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

    def extract_copyright(self, orig_case_full_path, exif_metadata, file_key):

        if file_key['CopyrightHolder'] is not None:
            self.copyright = file_key['CopyrightHolder']
            return 'file key'

        if exif_metadata:
            copyright = self.get_copyright_from_exif(exif_metadata)
            if copyright is not None and copyright.lower() != 'copyright':
                self.copyright = copyright

                return 'exif'

        orig_case_directory = os.path.dirname(orig_case_full_path)
        orig_case_filename = os.path.basename(orig_case_full_path)
        self.copyright = None


        if self.attempt_directory_copyright_extraction(orig_case_directory):
            return 'directory'

        filename_copyright = self.extract_copyright_from_string(orig_case_filename)
        if filename_copyright is not None:
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
                self.title = found_substring
                self.casiz_numbers = list(set([int(num) for num in re.findall(r'\b\d+\b', found_substring)]))
                return True
            if re.search(self.iz_importer_config.DIRECTORY_MATCH, cur_directory):
                casiz = self.extract_casiz(directory)
                self.title = f'CASIZ {casiz}'
                self.casiz_numbers = [casiz]
                return True
        return False

    def attempt_filename_match(self, full_path):
        filename = os.path.basename(full_path)

        # Check for conjunction matches first
        match = re.search(self.iz_importer_config.FILENAME_CONJUNCTION_MATCH, filename)
        if match:
            matched_string = match.group(0)

            # Find all CASIZ numbers
            casiz_numbers = re.findall(rf'{self.iz_importer_config.CASIZ_PREFIX}\d+', matched_string)

            # Extract the conjunctions "and" or "or"
            conjunctions = re.findall(r'\b(and|or)\b', matched_string, re.IGNORECASE)

            # Combine CASIZ numbers and conjunctions
            combined_result = ' '.join(
                [f'{num} {conjunction.upper()}' for num, conjunction in zip(casiz_numbers, conjunctions)] + [
                    casiz_numbers[-1]])

            self.title = combined_result
            # Joe untested as of 5/23/24
            # Extract all numeric groups (CASIZ numbers) from the match
            self.casiz_numbers = list(set([int(num) for num in re.findall(r'\b\d{5,12}\b', filename)]))
            print(f"Matched conjunction on {filename}. IDs: {self.casiz_numbers}")
            return True

        # Fallback to simple filename match
        match = re.search(self.iz_importer_config.FILENAME_MATCH, filename)
        if match:
            # Extract CASIZ number using the specific extraction method
            casiz_number = self.extract_casiz(filename)
            self.title=f"CASIZ {casiz_number}"
            if casiz_number is not None:
                self.casiz_numbers = [casiz_number]
                return True

        return False

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

    def include_by_extension(self, filepath: str) -> bool:

        pattern = re.compile(f'^.*{self.iz_importer_config.IMAGE_SUFFIX}')

        return bool(pattern.match(filepath))

    def check_already_in_image_db(self, full_path):
        if self.image_client.check_image_db_if_filename_imported(self.collection_name,
                                                                 full_path,
                                                                 exact=True):
            return True
        return False

    def build_filename_map(self, full_path):
        if not self._check_and_increment_counter():
            return False

        orig_case_full_path = full_path
        full_path = full_path.lower()

        if not self._validate_path(full_path):
            return False

        filename = os.path.basename(full_path)

        if self._should_skip_file(filename, full_path):
            return False

        if self._is_file_already_processed(full_path, orig_case_full_path):
            return False

        exif_metadata = self._read_exif_metadata(full_path)

        # sets self.title
        casiz_source = self.get_casiz_ids(full_path, exif_metadata)

        if not casiz_source:
            return False

        file_key = self._read_file_key(full_path)
        if file_key is None:
            return False

        print(f"Filekey: {file_key}")
        print(f"exif_metadata: {exif_metadata}")

        # sets self.copyright
        copyright_method = self.extract_copyright(orig_case_full_path, exif_metadata, file_key)

        # stores self.copyright, self.title, and selected info
        # into the metadata map.
        self._update_metadata_map(full_path, exif_metadata, orig_case_full_path, file_key)
        self._update_casiz_filepath_map(full_path)

        self.log_file_status(
            filename=os.path.basename(orig_case_full_path),
            path=orig_case_full_path,
            casiznumber_method=casiz_source,
            id=self.casiz_numbers,
            copyright_method=copyright_method,
            copyright=self.copyright
        )

        return True


    # Helper function to parse date
    def _parse_date(self, date_str):
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            try:
                return datetime.strptime(date_str, '%m/%d/%Y').date()
            except ValueError:
                try:
                    return datetime.strptime(date_str, '%B %d, %Y').date()
                except ValueError:
                    return None

    # Helper function to convert isPublic to boolean
    def _parse_boolean(self, value):
        return value.lower() == 'true' if value else False

    def _read_file_key(self, image_path):
        # Function to find the key.csv file in the current or parent directories
        def find_key_file(directory):
            while directory != os.path.dirname(directory):  # Stop at the root directory
                key_file_path = os.path.join(directory, 'key.csv')
                if os.path.isfile(key_file_path):
                    return key_file_path
                directory = os.path.dirname(directory)
            return None

        # Get the directory of the image file
        directory = os.path.dirname(image_path)

        # Find the key.csv file
        key_file_path = find_key_file(directory)
        if not key_file_path:
            self.log_file_status(
                filename=os.path.basename(image_path),
                path=image_path,
                rejected="Missing key.csv"
            )
            return None

        # Define the column mappings
        column_mappings = {
            'copyrightdate': 'CopyrightDate',
            'copyrightholder': 'CopyrightHolder',
            'credit': 'Credit',
            'license': 'License',
            'remarks': 'Remarks',
            'ispublic': 'IsPublic',
            'subtype': 'subType',
            'createdbyagent': 'createdByAgent',
            'metadatatext': 'creator'
        }

        # Initialize the dictionary
        result_dict = {}

        for mapped_key in column_mappings.values():
            result_dict[mapped_key] = None


        # Read the key.csv file with appropriate encoding
        try:
            with open(key_file_path, encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)
        except UnicodeDecodeError:
            with open(key_file_path, encoding='latin1') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)

        # Populate the dictionary with data from the key.csv file
        for row in data:
            if row and len(row) > 1:
                key = row[0].strip().lower()
                if key in column_mappings:
                    value = row[1].strip() if len(row) > 1 and row[1].strip() else None
                    if key == 'copyrightdate' and value:
                        value = self._parse_date(value)
                    elif key == 'ispublic' and value:
                        value = self._parse_boolean(value)
                    result_dict[column_mappings[key]] = value

        return result_dict

    def _check_and_increment_counter(self):
        if 'counter' not in globals():
            globals()['counter'] = 0
        if globals()['counter'] < 110:
            globals()['counter'] += 1
            return True
        else:
            return False

    def _validate_path(self, full_path):
        if 'crrf' in full_path:
            print("Rejecting all CRRF for now - pending mapping")
            self.log_file_status(
                filename=os.path.basename(full_path),
                path=full_path,
                rejected="Skipping CRRF for now"
            )
            return False

        if not self.include_by_extension(full_path):
            print(f"Will not import, excluded extension: {full_path}")
            self.log_file_status(
                filename=os.path.basename(full_path),
                path=full_path,
                rejected="Forbidden extension"
            )
            return False

        return True

    def _should_skip_file(self, filename, full_path):
        if filename.startswith('.'):
            print(f"Skipping all files that start with .: {full_path}")
            self.log_file_status(
                filename=filename,
                path=full_path,
                rejected=".filename"
            )
            return True
        return False

    def _is_file_already_processed(self, full_path, orig_case_full_path):
        if self.check_already_attached(full_path):
            self.log_file_status(
                filename=os.path.basename(full_path),
                path=full_path,
                rejected="Already imported"
            )
            return True

        if self.check_already_in_image_db(full_path):
            print(f"Already in image db {orig_case_full_path}")
            return True

        return False

    def _read_exif_metadata(self, full_path):
        exif_tools = MetadataTools(full_path)
        if exif_tools is not None:
            return exif_tools.read_exif()
        return None

    def get_casiz_ids(self, full_path, exif_metadata):
        # filename


        if self.attempt_filename_match(full_path):
            return 'Filename'

        if self.get_casiz_from_exif(exif_metadata) is not None:
            return 'EXIF'

        if self.attempt_directory_match(full_path):
            return 'Directory'



        self.log_file_status(
            filename=os.path.basename(full_path),
            path=full_path,
            rejected="no casiz match for exif, filename, or directory."
        )
        return None

    def _update_metadata_map(self, full_path, exif_metadata, orig_case_full_path, file_key):

        # Extract the 4-digit year from EXIF:CreateDate if available
        exif_create_date = exif_metadata.get('EXIF:CreateDate', '')
        exif_create_year = None
        if exif_create_date:
            exif_match = re.search(r'\b\d{4}\b', exif_create_date)
            if exif_match:
                exif_create_year = exif_match.group(0)

        # Extract the 4-digit year from file_key['CopyrightDate'] if available
        file_key_copyright_date = file_key.get('CopyrightDate', '')
        file_key_copyright_year = None
        if file_key_copyright_date:
            file_key_match = re.search(r'\b\d{4}\b', file_key_copyright_date)
            if file_key_match:
                file_key_copyright_year = file_key_match.group(0)

        # Determine the value for copyright_date
        if file_key_copyright_year:
            copyright_date = file_key_copyright_year
        else:
            copyright_date = exif_create_year

        # If both are null or zero-length strings, set to None
        if not copyright_date:
            copyright_date = None

        if 'IsPublic' not in file_key or file_key['IsPublic'] is None:
            file_key['IsPublic'] = False

        self.filepath_metadata_map[full_path] = {
            "copyright_date": copyright_date,
            "copyright_holder": self.copyright,
            "credit": file_key['Credit'],
            "date_imaged": exif_metadata.get('EXIF:CreateDate'),
            "license": file_key['License'],
            "remarks": file_key['Remarks'],
            "title": self.title,
            "is_public": file_key['IsPublic'],
            "metadata_text": file_key['creator'],
            "subtype": file_key['subType'],
            "type": 'StillImage',
            "original_filename": full_path,
            "created_by_agent_id": file_key['createdByAgent']
        }

        # self.filepathpath_metadata_map[full_path] = {
        #     "copyright": self.copyright,
        #     "exif_metadata": exif_metadata,
        #     "orig_case_full_path": orig_case_full_path
        # }

    def _update_casiz_filepath_map(self, full_path):
        self.casiz_numbers = list(
            map(lambda x: int(x) if str(x).isdigit() else int(''.join(filter(str.isdigit, str(x)))),
                self.casiz_numbers)
        )

        for cur_casiz_number in self.casiz_numbers:
            if cur_casiz_number not in self.casiz_filepath_map:
                self.casiz_filepath_map[cur_casiz_number] = [full_path]
            else:
                self.casiz_filepath_map[cur_casiz_number].append(full_path)
