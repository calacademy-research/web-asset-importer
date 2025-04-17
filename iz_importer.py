import os
import re
import csv
import logging
import warnings

from datetime import datetime
from importer import Importer
from directory_tree import DirectoryTree
from metadata_tools.metadata_tools import MetadataTools

from time_utils import get_pst_time_now_string
from get_configs import get_config
from metadata_tools.EXIF_constants import EXIFConstants
from specify_constants import SpecifyConstants

logging.basicConfig(level=logging.WARNING)

CASIZ_FILE_LOG = "file_log.tsv"
starting_time_stamp = datetime.now()


class AgentNotFoundException(Exception):
    pass


class IzImporter(Importer):
    class ItemMapping:
        def __init__(self):
            self.casiz_numbers = []

    def __init__(self):
        warnings.filterwarnings("ignore", category=UserWarning)
        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')
        self._configure_logging()

        self.iz_importer_config = get_config(config="IZ")
        self.AGENT_ID = self.iz_importer_config.AGENT_ID
        self.log_file = open(CASIZ_FILE_LOG, "w+")
        self.item_mappings = []
        self._initialize_log_file()
        self.casiz_numbers = []
        self.title = ""

        self.filepath_metadata_map = {}
        self.collection_name = self.iz_importer_config.COLLECTION_NAME

        super().__init__(self.iz_importer_config, "Invertebrate Zoology")
        self.casiz_filepath_map = {}

    def import_files(self, IZ_SCAN_FOLDERS=None):
        if not IZ_SCAN_FOLDERS:
            IZ_SCAN_FOLDERS = self.iz_importer_config.IZ_SCAN_FOLDERS
        self.directory_tree_core = DirectoryTree(IZ_SCAN_FOLDERS, pickle_for_debug=False)
        self.directory_tree_core.process_files(self.build_filename_map)
        print("Starting to process loaded core files...")
        self.process_loaded_files()

        if self.iz_importer_config.MAILING_LIST:
            image_dict = self.image_client.monitoring_dict
            self.image_client.monitoring_tools.send_monitoring_report(subject=f"IZ_BATCH:{get_pst_time_now_string()}",
                                                                      time_stamp=starting_time_stamp,
                                                                      image_dict=image_dict)
    def _configure_logging(self):
        logging.getLogger('Client.dbutils').setLevel(logging.WARNING)
        logging.getLogger('Client.importer').setLevel(logging.DEBUG)
        logging.getLogger('Client.ImageClient').setLevel(logging.DEBUG)
        logging.getLogger('Client.IzImporter').setLevel(logging.DEBUG)

    def _initialize_log_file(self):
        self.log_file.write(f"casiz\tfilename\tCASIZ method\tcopyright method\tcopyright\trejected\tpath on disk\n")

    def process_loaded_files(self):
        for casiz_number in self.casiz_filepath_map.keys():
            filepaths = self.casiz_filepath_map[casiz_number]
            filepath_list = [cur_filepath for cur_filepath in filepaths]
            self.process_casiz_number(casiz_number, filepath_list)

    def process_casiz_number(self, casiz_number, filepath_list):
        self.logger.debug(f"Processing casiz_numbers: {casiz_number}")
        sql = f"select collectionobjectid from collectionobject where catalognumber= %s"
        params = (casiz_number,)
        collection_object_id = self.specify_db_connection.get_one_record(sql, params=params)
        if collection_object_id is None:
            print(f"No record found for casiz_number {casiz_number}, skipping.")
            return

        filepath_list = self.remove_specify_imported_and_id_linked_from_path(filepath_list, collection_object_id)

        for cur_filepath in filepath_list:
            if not os.path.exists(cur_filepath):
                self.logger.warning(f"File not found - possibly moved after start of ingest: {cur_filepath}, skipping.")
                continue
            attachment_id = self.attachment_utils.get_attachmentid_from_filepath(cur_filepath)
            if attachment_id is not None:
                self.connect_existing_attachment_to_collection_object_id(attachment_id, collection_object_id,
                                                                         self.AGENT_ID)
            else:
                attachment_properties_map = self.filepath_metadata_map[cur_filepath]
                agent = attachment_properties_map.get(SpecifyConstants.ST_CREATED_BY_AGENT_ID) or self.AGENT_ID
                is_public = attachment_properties_map[SpecifyConstants.ST_IS_PUBLIC]
                # see comments in import_single_file_to_image_db_and_specify;
                # This is a bit silly but we're faking up the "redact" flag using the logic here.
                self.logger.debug(f"importing single file: {cur_filepath}")

                attach_loc = self.import_single_file_to_image_db_and_specify(cur_filepath=cur_filepath,
                                                                             collection_object_id=collection_object_id,
                                                                             agent_id=agent,
                                                                             skip_redacted_check=is_public,
                                                                             attachment_properties_map=attachment_properties_map,
                                                                             force_redacted=not is_public,
                                                                             id=casiz_number)
                self.logger.debug(f"importing single file COMPLETE: {cur_filepath}")

                if attach_loc is None:
                    self.logger.error(f"Failed to upload image, aborting upload for {cur_filepath}")
                    return
                self.image_client.write_exif_image_metadata(self._get_exif_mapping(attachment_properties_map),
                                                            self.collection_name, attach_loc)

                md = MetadataTools(path=cur_filepath)
                md.write_exif_tags(exif_dict=self._get_exif_mapping(attachment_properties_map), overwrite_blank=True)

    def _get_exif_mapping(self, attachment_properties_map):

        exif_mapping = {
            EXIFConstants.EXIF_ARTIST: attachment_properties_map.get(SpecifyConstants.ST_METADATA_TEXT),
            EXIFConstants.EXIF_CREATE_DATE: attachment_properties_map.get(SpecifyConstants.ST_DATE_IMAGED),
            EXIFConstants.EXIF_IMAGE_DESCRIPTION: attachment_properties_map.get(SpecifyConstants.ST_TITLE),
            EXIFConstants.IPTC_CREDIT: None,
            EXIFConstants.IPTC_COPYRIGHT_NOTICE: attachment_properties_map.get(SpecifyConstants.ST_COPYRIGHT_HOLDER),
            EXIFConstants.IPTC_BY_LINE: attachment_properties_map.get(SpecifyConstants.ST_METADATA_TEXT),
            EXIFConstants.IPTC_CAPTION_ABSTRACT: attachment_properties_map.get(SpecifyConstants.ST_TITLE),
            EXIFConstants.XMP_CREDIT: attachment_properties_map.get(SpecifyConstants.ST_CREDIT),
            EXIFConstants.XMP_CREATOR: attachment_properties_map.get(SpecifyConstants.ST_METADATA_TEXT),
            EXIFConstants.XMP_USAGE: attachment_properties_map.get(SpecifyConstants.ST_LICENSE),
            EXIFConstants.XMP_USAGE_TERMS: attachment_properties_map.get(SpecifyConstants.ST_LICENSE),
            EXIFConstants.XMP_CREATE_DATE: attachment_properties_map.get(SpecifyConstants.ST_FILE_CREATED_DATE),
            EXIFConstants.XMP_TITLE: attachment_properties_map.get(SpecifyConstants.ST_TITLE),
            EXIFConstants.XMP_DATE_CREATED: attachment_properties_map.get(SpecifyConstants.ST_DATE_IMAGED),

            # New
            EXIFConstants.EXIF_COPYRIGHT: attachment_properties_map.get(SpecifyConstants.ST_COPYRIGHT_HOLDER),
            EXIFConstants.XMP_RIGHTS: attachment_properties_map.get(SpecifyConstants.ST_COPYRIGHT_HOLDER),
            EXIFConstants.IFD0_COPYRIGHT: attachment_properties_map.get(SpecifyConstants.ST_COPYRIGHT_HOLDER),

            EXIFConstants.XMP_RIGHTS_USAGE_TERMS: attachment_properties_map.get(SpecifyConstants.ST_LICENSE),
            EXIFConstants.XMP_PLUS_IMAGE_SUPPLIER_NAME: attachment_properties_map.get(SpecifyConstants.ST_CREDIT),
            EXIFConstants.PHOTOSHOP_CREDIT: attachment_properties_map.get(SpecifyConstants.ST_CREDIT),

        }

        # Remove keys with None values
        return {k: v for k, v in exif_mapping.items()}

    def log_file_status(self, id=None, filename=None, path=None, casiznumber_method=None, rejected=None,
                        copyright_method=None, copyright=None, conjunction=None):
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
            f"Logged: {id} copyright method: {copyright_method} copyright: '{copyright}' rejected:{rejected} filename: {filename} Path: {path}")
        self.log_file.write(
            f"{id}\t{filename}\t{casiznumber_method}\t{copyright_method}\t{copyright}\t{rejected}\t{path}\n")

    def extract_casiz_number(self, candidate_string, pattern):
        match = re.search(pattern, candidate_string)
        if match:
            casiz_number = re.search(r'\d+', match.group())
            if casiz_number:
                return int(casiz_number.group())
        return None

    def extract_exact_casiz_match(self, candidate_string):
        match = None
        if re.search(r'casiz', candidate_string, re.IGNORECASE):
            match = re.search(self.iz_importer_config.CASIZ_NUMBER_EXACT, candidate_string, re.IGNORECASE)
        else:
            match = re.search(self.iz_importer_config.CASIZ_MATCH, candidate_string)

        return match

    def extract_casiz_single(self, candidate_string):
        return self.extract_casiz_number(candidate_string, self.iz_importer_config.CASIZ_NUMBER)

    def extract_casiz_from_string(self, input_string):
        match = re.search(self.iz_importer_config.FILENAME_CONJUNCTION_MATCH, input_string)
        if match:
            integers = set()
            matches = re.finditer(self.iz_importer_config.FILENAME_CONJUNCTION_MATCH, input_string)

            for match in matches:
                numbers = re.findall(r'\d+', match.group())
                for number in numbers:
                    integers.add(int(number))

            self.casiz_numbers=list(integers)

            self.title = os.path.splitext(input_string)[0]
            if len(self.casiz_numbers) > 0:
                return True

        match = re.search(self.iz_importer_config.CASIZ_MATCH, input_string)

        if match:
            exact = self.extract_exact_casiz_match(input_string)
            if exact is not None:
                self.casiz_numbers = [exact]
                return True
            else:
                print(f"No exact match found for {input_string}")

        casiz_number = self.extract_casiz_single(input_string)
        if casiz_number is not None:
            self.casiz_numbers = [casiz_number]
            self.title = os.path.splitext(input_string)[0]
            return True

        return False

    def extract_copyright_from_string(self, copyright_string):
        if not copyright_string:
            return None
        copyright = None

        # Case-insensitive search for 'copyright' and literal '©'
        lower_str = copyright_string.lower()
        idx_c = lower_str.rfind('copyright')
        idx_sym = copyright_string.rfind('©')

        # Determine which one comes last in the string
        if idx_c == -1 and idx_sym == -1:
            return None
        elif idx_c > idx_sym:
            # Extract after 'copyright'
            copyright = copyright_string[idx_c + len('copyright'):]
        else:
            # Extract after '©'
            copyright = copyright_string[idx_sym + 1:]

        # Clean it up
        if copyright is not None:
            copyright = copyright.strip()
            copyright = re.sub(r'\s*_.*$', '', copyright)

        return copyright


    def get_casiz_from_exif(self, exif_metadata):
        priority_tags = [
            EXIFConstants.IPTC_KEYWORDS,
            EXIFConstants.XMP_DC_SUBJECT,
            EXIFConstants.XMP_LR_HIERARCHICAL_SUBJECT,
            EXIFConstants.IPTC_CAPTION_ABSTRACT,
            EXIFConstants.XMP_DC_DESCRIPTION,
            EXIFConstants.EXIF_IFD0_IMAGE_DESCRIPTION,
            EXIFConstants.XMP_TITLE
        ]

        if exif_metadata is None:
            return None

        for tag in priority_tags:
            if tag in exif_metadata:
                possible_description = exif_metadata[tag].strip()
                possible_description = possible_description.lower()
                if self.extract_casiz_from_string(possible_description):
                    return self.casiz_numbers

        return None

    def get_copyright_from_exif(self, exif_metadata):
        if exif_metadata is None:
            return None
        copyright_keys = ["EXIF:Copyright", "IPTC:CopyrightNotice"]
        for key in copyright_keys:
            if key in exif_metadata:
                copyright = exif_metadata[key].strip()
                if copyright.startswith('Â'):
                    copyright = copyright[1:]
                if len(copyright) <= 2 or "\x00\x00\x00\x00\x00\x00\x00" in copyright:
                    return None
                return copyright
        return None

    def extract_copyright(self, orig_case_full_path, exif_metadata, file_key):
        if file_key is not None and 'CopyrightHolder' in file_key and \
            file_key['CopyrightHolder'] is not None:
            self.copyright = file_key['CopyrightHolder']
            return 'file key'

        if exif_metadata:
            copyright = self.get_copyright_from_exif(exif_metadata)
            if copyright is not None and copyright.lower() != 'copyright':
                self.copyright = copyright
                return 'exif'

        orig_case_directory = os.path.dirname(orig_case_full_path)
        self.copyright = None

        if self.attempt_directory_copyright_extraction(orig_case_directory):
            return 'directory'

        filename_copyright = self.extract_copyright_from_string(os.path.basename(orig_case_full_path))
        if filename_copyright is not None:
            self.copyright = filename_copyright
            return 'filename'

        return None

    def attempt_directory_match(self, full_path):
        directory = os.path.dirname(full_path)
        directories = directory.split('/')
        for cur_directory in reversed(directories):
            for pattern in [self.iz_importer_config.DIRECTORY_CONJUNCTION_MATCH,
                            self.iz_importer_config.DIRECTORY_MATCH]:
                result = re.search(pattern, cur_directory)
                if result:
                    found_substring = result.groups()[0]
                    self.title = cur_directory
                    if pattern == self.iz_importer_config.DIRECTORY_CONJUNCTION_MATCH:
                        self.casiz_numbers = list(set(map(int, re.findall(rf'\b\d{{{self.iz_importer_config.SHORT_MINIMUM_ID_DIGITS},{self.iz_importer_config.MAXIMUM_ID_DIGITS}}}\b', found_substring))))
                    else:
                        casiz_number = self.extract_casiz_single(cur_directory)
                        if casiz_number is not None:
                            self.casiz_numbers = [casiz_number]
                    return True
        return False

    def attempt_filename_match(self, full_path):
        filename = os.path.basename(full_path)
        return self.extract_casiz_from_string(filename)

    def attempt_directory_copyright_extraction(self, directory_orig_case):
        directories = directory_orig_case.split('/')
        for cur_directory in reversed(directories):
            copyright = self.extract_copyright_from_string(cur_directory)
            if copyright is not None:
                self.copyright = copyright
                return True
        return False

    def include_by_extension(self, filepath: str) -> bool:
        pattern = re.compile(f'^.*{self.iz_importer_config.IMAGE_SUFFIX}')
        return bool(pattern.match(filepath))

    def build_filename_map(self, full_path):
        self._check_and_increment_counter()

        orig_case_full_path = full_path
        full_path = full_path.lower()

        if not self.validate_path(full_path):
            return False

        filename = os.path.basename(full_path)
        if self._should_skip_file(filename, full_path):
            return False
        file_key = self._read_file_key(full_path)

        if file_key and str(file_key.get('remove', '')).lower() == 'true':
            self.logger.info(f"Marked for removal: {full_path}")
            self.remove_file_from_database(full_path)
            return False

        if self._is_file_already_processed(full_path, orig_case_full_path):
            return False

        exif_metadata = self._read_exif_metadata(full_path)
        casiz_source = self.get_casiz_ids(full_path, exif_metadata)
        if not casiz_source:
            return False

        copyright_method = self.extract_copyright(orig_case_full_path, exif_metadata, file_key)
        try:
            self._update_metadata_map(full_path, exif_metadata, file_key)
        except AgentNotFoundException as e:
            self.log_file_status(filename=os.path.basename(full_path), path=full_path, rejected="Can't locate agent {e}".format(e=e))
            return False

        self._update_casiz_filepath_map(full_path)

        self.log_file_status(filename=os.path.basename(orig_case_full_path), path=orig_case_full_path,
                             casiznumber_method=casiz_source, id=self.casiz_numbers, copyright_method=copyright_method,
                             copyright=self.copyright)
        return True


    def _check_and_increment_counter(self):
        if 'counter' not in globals():
            globals()['counter'] = 0

        globals()['counter'] += 1


    def validate_path(self, full_path):
        if 'crrf' in full_path:
            print("Rejecting all CRRF for now - pending mapping")
            self.log_file_status(filename=os.path.basename(full_path), path=full_path, rejected="Skipping CRRF for now")
            return False

        if not self.include_by_extension(full_path):
            print(f"Will not import, excluded extension: {full_path}")
            self.log_file_status(filename=os.path.basename(full_path), path=full_path, rejected="Forbidden extension")
            return False

        return True

    def _should_skip_file(self, filename, full_path):
        if filename.startswith('.'):
            print(f"Skipping all files that start with .: {full_path}")
            self.log_file_status(filename=filename, path=full_path, rejected=".filename")
            return True
        return False

    def _is_file_already_processed(self, full_path, orig_case_full_path):

        if self.attachment_utils.get_attachmentid_from_filepath(full_path) is not None:
            self.log_file_status(filename=os.path.basename(full_path), path=full_path, rejected="Already imported")
            return True
        if self.image_client.check_image_db_if_filename_imported(self.collection_name, full_path, exact=True):
            print(f"Already in image db {orig_case_full_path}")
            return True

        return False

    def _read_exif_metadata(self, full_path):
        exif_tools = MetadataTools(full_path)
        if exif_tools is not None:
            return exif_tools.read_exif_tags()
        return None

    def get_casiz_ids(self, full_path, exif_metadata):
        if self.attempt_filename_match(full_path):
            return 'Filename'

        if self.get_casiz_from_exif(exif_metadata) is not None:
            return 'EXIF'

        if self.attempt_directory_match(full_path):
            return 'Directory'

        self.log_file_status(filename=os.path.basename(full_path), path=full_path,
                             rejected="no casiz match for exif, filename, or directory.")
        return None

    def _update_metadata_map(self, full_path, exif_metadata, file_key):
        exif_create_date = exif_metadata.get('EXIF:CreateDate', '')
        exif_create_year = self._extract_year_from_date(exif_create_date)
        file_key_copyright_year = None
        agent_id = None

        if file_key is not None:
            if 'CopyrightDate' in file_key:
                file_key_copyright_date = file_key.get('CopyrightDate', '')
                file_key_copyright_year = self._extract_year_from_date(file_key_copyright_date)

            if 'IsPublic' not in file_key or file_key['IsPublic'] is None or file_key['IsPublic'] is False:
                file_key['IsPublic'] = False
            else:
                file_key['IsPublic'] = True

            if 'creator' in file_key and file_key['creator'] is not None and len(file_key['creator']) > 1:
                agent_id = self.find_agent_id_from_string(file_key['creator'])

            copyright_date = file_key_copyright_year if file_key_copyright_year is not None else exif_create_year or None

            # This gets passed to import_single_file_to_image_db_and_specify and set in specify.
            self.filepath_metadata_map[full_path] = {
                SpecifyConstants.ST_COPYRIGHT_DATE: copyright_date,
                SpecifyConstants.ST_COPYRIGHT_HOLDER: self.copyright,
                SpecifyConstants.ST_CREDIT: file_key.get('Credit', ''),
                SpecifyConstants.ST_DATE_IMAGED: exif_metadata.get('EXIF:CreateDate'),
                SpecifyConstants.ST_LICENSE: file_key.get('License', ''),
                SpecifyConstants.ST_REMARKS: file_key.get('Remarks', ''),
                SpecifyConstants.ST_TITLE: self.title,
                SpecifyConstants.ST_IS_PUBLIC: file_key['IsPublic'],
                SpecifyConstants.ST_SUBTYPE: file_key.get('subType', ''),
                SpecifyConstants.ST_TYPE: 'StillImage',
                SpecifyConstants.ST_ORIG_FILENAME: full_path,
                SpecifyConstants.ST_CREATED_BY_AGENT_ID: file_key.get('createdByAgent', ''),
                SpecifyConstants.ST_METADATA_TEXT: file_key.get('creator', '')
            }
            if agent_id is not None:
                self.filepath_metadata_map[full_path][SpecifyConstants.ST_CREATOR_ID] = agent_id
        else:
            copyright_date = exif_create_year or None

            # This gets passed to import_single_file_to_image_db_and_specify and set in specify.
            self.filepath_metadata_map[full_path] = {
                SpecifyConstants.ST_COPYRIGHT_DATE: copyright_date,
                SpecifyConstants.ST_COPYRIGHT_HOLDER: self.copyright,
                SpecifyConstants.ST_CREDIT: '',
                SpecifyConstants.ST_DATE_IMAGED: exif_metadata.get('EXIF:CreateDate'),
                SpecifyConstants.ST_LICENSE: '',
                SpecifyConstants.ST_REMARKS: '',
                SpecifyConstants.ST_TITLE: self.title,
                SpecifyConstants.ST_IS_PUBLIC: False,
                SpecifyConstants.ST_SUBTYPE: '',
                SpecifyConstants.ST_TYPE: 'StillImage',
                SpecifyConstants.ST_ORIG_FILENAME: full_path,
                SpecifyConstants.ST_CREATED_BY_AGENT_ID: '',
                SpecifyConstants.ST_METADATA_TEXT: ''
            }

    def find_agent_id_from_string(self, input_string, agents=None):
        import difflib

        def fuzzy_match(query, choices, cutoff=0.8):
            matches = difflib.get_close_matches(query, choices, n=1, cutoff=cutoff)
            return matches[0] if matches else None

        # Split the input string into firstname and lastname
        names = input_string.strip().split()
        if len(names) < 2:
            return None

        firstname = names[0].lower()
        lastname = names[-1].lower()

        if agents is None:
            # Get a list of possible agent names from the database
            sql = "SELECT AgentID, FirstName, LastName FROM casiz.agent"
            cursor = self.specify_db_connection.get_cursor()
            cursor.execute(sql)
            agents = cursor.fetchall()
            cursor.close()

        # Normalize agent names for comparison
        agent_names = [(agent[0], agent[1].lower() if agent[1] else '', agent[2].lower() if agent[2] else '') for agent
                       in agents]

        # Find a fuzzy match for the firstname and lastname
        possible_firstnames = [agent[1] for agent in agent_names]
        possible_lastnames = [agent[2] for agent in agent_names]

        matched_firstname = fuzzy_match(firstname, possible_firstnames)
        matched_lastname = fuzzy_match(lastname, possible_lastnames)

        if matched_firstname and matched_lastname:
            for agent_id, fname, lname in agent_names:
                if fname == matched_firstname and lname == matched_lastname:
                    return agent_id

        return None

    def _extract_year_from_date(self, date_str):
        if date_str is not None:
            match = re.search(r'\b\d{4}\b', date_str)
            if match:
                return match.group(0)
        return None

    def _update_casiz_filepath_map(self, full_path):
        self.casiz_numbers = list(
            map(lambda x: int(x) if str(x).isdigit() else int(''.join(filter(str.isdigit, str(x)))),
                self.casiz_numbers))

        for cur_casiz_number in self.casiz_numbers:
            if cur_casiz_number not in self.casiz_filepath_map:
                self.casiz_filepath_map[cur_casiz_number] = [full_path]
            else:
                self.casiz_filepath_map[cur_casiz_number].append(full_path)
    @staticmethod
    def find_key_file(directory):
        while directory != os.path.dirname(directory):
            key_file_path = os.path.join(directory, 'key.csv')
            if os.path.isfile(key_file_path):
                return key_file_path
            directory = os.path.dirname(directory)
        return None

    def _read_file_key(self, image_path):
        directory = os.path.dirname(image_path)
        key_file_path = IzImporter.find_key_file(directory)
        if not key_file_path:
            self.log_file_status(filename=os.path.basename(image_path), path=image_path, rejected="Missing key.csv")
            return None

        # returned_dict:file_based_key_value
        column_mappings = {
            'copyrightdate': 'CopyrightDate',
            'copyrightholder': 'CopyrightHolder',
            'credit': 'Credit',
            'license': 'License',
            'remarks': 'Remarks',
            'ispublic': 'IsPublic',
            'subtype': 'subType',
            'createdbyagent': 'createdByAgent',
            'metadatatext': 'creator',
            'remove': 'remove'

        }

        result_dict = {mapped_key: None for mapped_key in column_mappings.values()}

        try:
            with open(key_file_path, encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)
        except UnicodeDecodeError:
            with open(key_file_path, encoding='latin1') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)

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

    def _parse_date(self, date_str):
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y'):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    def _parse_boolean(self, value):
        return value.lower() == 'true' if value else False
