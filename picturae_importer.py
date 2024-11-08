"""this takes the csv data from picturae_csv_create, and parses the fields on a row by row basis into new db records,
    filling in the necessary relational tables, and uploading attached images to image server.
    Used as part of picturae_project for upload post-imaging, to create records to be updated post OCR and transcription
"""
import atexit
import os.path
import shutil
from uuid import uuid4
from gen_import_utils import *
import logging
from string_utils import *
from importer import Importer
from sql_csv_utils import SqlCsvTools
from botany_importer import BotanyImporter
from get_configs import get_config
import time_utils
import logging.handlers
from monitoring_tools_derived import MonitoringToolsDir
from taxon_tools.BOT_TNRS import process_taxon_resolve

starting_time_stamp = datetime.now()

class PicturaeImporter(Importer):
    """DataOnboard:
           A class with methods designed to wrangle, verify,
           and upload a csv file containing transcribed
           specimen sheet records into the database,
           along with attached images
    """

    def __init__(self, config):

        self.picturae_config = config

        super().__init__(self.picturae_config, "Botany")

        self.picdb_config = get_config(config='picbatch')

        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)

        self.csv_folder = self.picturae_config.CSV_FOLDER

        self.botany_importer = None

        self.process_csv_files()

        self.init_all_vars()

        self.num_barcodes = len(self.record_full)

        self.run_all_methods()


    def process_csv_files(self):

        self.csv_folder = self.picturae_config.CSV_FOLDER

        self.file_path = None
        max_digits = -1

        for file in os.listdir(self.csv_folder):
            if file.endswith('.csv'):
                full_file_path = os.path.join(self.csv_folder, file)
                file_digits = int(remove_non_numerics(file))
                if file_digits > max_digits:
                    max_digits = file_digits
                    self.file_path = full_file_path

        if self.file_path:
            self.logger.info(f"File with the highest numeric digits: {self.file_path}")
        else:
            raise ValueError("No CSV files found")


        self.record_full = pd.read_csv(self.file_path)

        # creating paths list for botany importer out of root directories in CSV file

        valid_paths = self.record_full[(self.record_full['image_valid'] == True) &
                                       (self.record_full['image_present_db'] == False)]['image_path']

        if len(valid_paths) > 0:
            paths = list(valid_paths.apply(os.path.dirname).unique())

            updated_paths = [os.path.join(self.picturae_config.PREFIX, path) for path in paths]

            self.paths = updated_paths
        else:
            self.paths = []


    def init_all_vars(self):
        """setting init variables:
            a list of variables and data structures to be initialized at the beginning of the class.
            args:
                date_string: the date input recieved from init params
                paths: the paths string recieved from the init params"""

        self.batch_md5 = generate_token(starting_time_stamp, self.file_path)

        self.record_full['batch_md5'] = self.batch_md5
        if self.picturae_config.MAILING_LIST:
            self.monitoring_tools = MonitoringToolsDir(config=self.picturae_config,
                                                       batch_md5=self.batch_md5,
                                                       report_path=self.picturae_config.ACTIVE_REPORT_PATH,
                                                       active=True)

        # setting up db sql_tools for each connection

        self.sql_csv_tools = SqlCsvTools(config=self.picturae_config, logging_level=self.logger.getEffectiveLevel())

        self.batch_sql_tools = SqlCsvTools(config=self.picdb_config, logging_level=self.logger.getEffectiveLevel())


        # full collector list is for populating existing and missing agents into collector table
        # new_collector_list is only for adding new agents to agent table.
        empty_lists = ['barcode_list', 'image_list', 'full_collector_list', 'new_collector_list',
                       'taxon_list', 'parent_list', 'new_taxa']

        for empty_list in empty_lists:
            setattr(self, empty_list, [])

        self.no_match_dict = {}

        # intializing parameters for database upload
        init_list = ['GeographyID', 'taxon_id', 'barcode',
                     'verbatim_date', 'start_date', 'end_date',
                     'collector_number', 'locality', 'collecting_event_guid',
                     'collecting_event_id', 'locality_guid', 'agent_guid',
                     'geography_string', 'GeographyID', 'locality_id',
                     'full_name', 'tax_name', 'locality',
                     'determination_guid', 'collection_ob_id', 'collection_ob_guid',
                     'name_id', 'author_sci', 'family', 'gen_spec_id', 'family_id', 'parent_author',
                     'redacted']

        for param in init_list:
            setattr(self, param, None)

        self.created_by_agent = self.picturae_config.IMPORTER_AGENT_ID


    def run_timestamps(self, batch_size: int):
        """updating md5 fields for new taxon and taxon mismatch batches"""
        ending_time_stamp = datetime.now()

        sql = self.batch_sql_tools.create_batch_record(start_time=starting_time_stamp, end_time=ending_time_stamp,
                                                       batch_md5=self.batch_md5, batch_size=batch_size,
                                                       agent_id=self.created_by_agent)

        self.batch_sql_tools.insert_table_record(sql=sql)


    def exit_timestamp(self):
        """time_stamper: runs timestamp at beginning of script and at exit,
                        uses the create_timestamps function from data_utils
                        to append timestamps with codes to csvs,
                        for record purging"""
        # marking starting time stamp
        # at exit run ending timestamp and append timestamp csv
        atexit.register(self.run_timestamps, batch_size=self.num_barcodes)
        atexit.register(self.unhide_files)


    def assign_col_dtypes(self):
        """just in case csv import changes column dtypes, resetting at top of file,
            re-standardizing null and nan records to all be pd.NA() and
            evaluate strings into booleans
        """
        # setting datatypes for columns
        string_list = self.record_full.columns.to_list()

        self.record_full[string_list] = self.record_full[string_list].astype(str)

        self.record_full = self.record_full.replace({'True': True, 'False': False}).infer_objects(copy=False)

        self.record_full = self.record_full.replace([None, 'nan', np.nan, '<NA>'], '')

        # removing leading apostrophes from data columns
        for col_name in list(["start", "end"]):
            self.record_full[f"{col_name}_date"] = self.record_full[f"{col_name}_date"].str.lstrip("\'")


    def duplicate_images(self):
        """will copy image associated with parent barcode,
           and rename according to new barcode for duplicate sheets"""

        for row in self.record_full.itertuples():
            if row.duplicate is True:
                parent_bar = row.parent_CatalogNumber

                parent_bar = re.sub(r"_[0-9]+", "", parent_bar)

                new_bar = row.CatalogNumber

                # constructing paths of new duplicate image
                new_image_path = os.path.dirname(row.image_path) + f"{os.path.sep}{new_bar}.tif"

                old_path = self.picturae_config.PREFIX + row.image_path

                new_path = self.picturae_config.PREFIX + new_image_path

                try:
                    if os.path.exists(new_path) is False and not row.image_present_db:
                        shutil.copy2(old_path, new_path)
                        self.logger.info(f"copy made of duplicate sheet {parent_bar}, at {new_bar}")
                    else:
                        self.logger.info(f"Copy of image for {parent_bar} already exists at {new_bar}")


                    self.record_full.loc[self.record_full['CatalogNumber'] == new_bar, 'image_path'] = new_image_path

                except Exception as e:
                    raise FileNotFoundError(f"Error: {e}")

            else:
                pass


    def create_file_list(self):
        """create_file_list: creates a list of imagepaths and barcodes for upload,
                                after checking conditions established to prevent
                                overwriting data functions
        """
        for row in self.record_full.itertuples(index=False):
            image_path = self.picturae_config.PREFIX + str(row.image_path)
            if not row.image_valid:
                raise ValueError(f"image {row.image_path} is not valid ")

            elif not row.is_barcode_match:
                raise ValueError(f"image barcode {row.image_path} does not match "
                                 f"{row.CatalogNumber}")

            elif row.barcode_present and row.image_present_db:
                self.logger.warning(f"record {row.CatalogNumber} and image {row.image_path}"
                                    f" already in database")

            elif row.barcode_present and not row.image_present_db:
                self.logger.warning(f"record {row.CatalogNumber} "
                                    f"already in database, appending image")
                self.image_list.append(image_path)

            elif not row.barcode_present and row.image_present_db:
                self.logger.warning(f"image {row.image_path} "
                                    f"already in database, appending record")
                self.barcode_list.append(row.CatalogNumber)
                # image path is added any-ways as the image client checks regardless
                # for image duplication, this way it will still create the attachment row

            else:
                self.image_list.append(image_path)
                self.barcode_list.append(row.CatalogNumber)

            self.barcode_list = list(set(self.barcode_list))
            self.image_list = list(set(self.image_list))
            # running unhide files at beginning just in case failed run



    def taxon_assign_defitem(self, taxon_string):
        """taxon_assign_defitme: assigns, taxon rank and treeitemid number,
                                based on subtrings present in taxon name.
            args:
                taxon_string: the taxon string or substring, which before assignment
        """
        def_tree = 13
        rank_id = 220
        if "subsp." in taxon_string:
            def_tree = 14
            rank_id = 230
        if "var." in taxon_string:
            def_tree = 15
            rank_id = 240
        if "subvar." in taxon_string:
            def_tree = 16
            rank_id = 250
        if " f. " in taxon_string:
            def_tree = 17
            rank_id = 260
        if "subf." in taxon_string:
            def_tree = 21
            rank_id = 270

        return def_tree, rank_id


    def taxa_author_tnrs(self, taxon_name, barcode):
        """taxa_author_tnrs: designed to take in one taxaname and
           do a TNRS operation on it to get an author for iterative higher taxa.

           args:
                taxon_name: a string of a taxon name, or a parsed genus or family, used to control
                            for unconfirmed species, and spelling mistakes.
                barcode: the string barcode of the taxon name associated with each photo.
                         used to re-merge dataframes after TNRS and keep track of the record in R.
        """

        taxon_frame = {"CatalogNumber": [barcode], "fullname": [taxon_name]}

        taxon_frame = pd.DataFrame(taxon_frame)

        # running taxonomic names through TNRS

        resolved_taxon = process_taxon_resolve(taxon_frame)

        taxon_list = list(resolved_taxon['matched_name_author'])

        self.parent_author = taxon_list[0]



    def create_agent_list(self, row):
        """create_agent_list:
                creates a list of collectors that will be checked and added to agent/collector tables.
                checks each collector first and last name against the database, and
                then if absent, appends the new agent name to a list of dictionaries self.collector_list.
           args:
                row: a dataframe row containing collector name information
        """
        self.new_collector_list = []
        self.full_collector_list = []

        column_names = list(self.record_full.columns)

        matches = sum([name.startswith("collector_first_name") for name in column_names])

        for i in range(1, matches+1):
            try:
                first = escape_apostrophes(getattr(row, f'collector_first_name{i}', ''))
                middle = escape_apostrophes(getattr(row, f'collector_middle_name{i}', ''))
                last = escape_apostrophes(getattr(row, f'collector_last_name{i}', ''))
                agent_id = escape_apostrophes(getattr(row, f'agent_id{i}', ''))

            except ValueError:
                break

            if agent_id and pd.notna(agent_id):
                # note do not convert agent_id to string it will mess with sql
                collector_dict = {f'collector_first_name': str(first),
                                  f'collector_middle_initial': str(middle),
                                  f'collector_last_name': str(last),
                                  f'collector_title': '',
                                  f'agent_id': agent_id}

                self.full_collector_list.append(collector_dict)


            elif any(not pd.isna(x) and x != '' and x is not None for x in [first, middle, last]):
                # first name title taking priority over last
                first_name, title_first = assign_collector_titles(first_last='first', name=f"{first}",
                                                                  config=self.picturae_config)



                last_name, title_last = assign_collector_titles(first_last='last', name=f"{last}",
                                                                config=self.picturae_config)

                if pd.notna(title_first) and title_first != '':
                    title = title_first
                else:
                    title = title_last

                middle = middle
                elements = [str(first_name).strip(), str(last_name).strip(), str(title).strip(), str(middle).strip()]

                for index in range(len(elements)):
                    if pd.isna(elements[index]) or elements[index] == '':
                        elements[index] = ''

                first_name, last_name, title, middle = elements

                agent_id = self.sql_csv_tools.check_agent_name_sql(first_name, last_name, middle, title)

                collector_dict = {f'collector_first_name': str(first_name),
                                  f'collector_middle_initial': str(middle),
                                  f'collector_last_name': str(last_name),
                                  f'collector_title': str(title),
                                  f'agent_id': agent_id}


                self.full_collector_list.append(collector_dict)

                if not agent_id or pd.isna(agent_id):
                    self.new_collector_list.append(collector_dict)

        self.full_collector_list = self.sql_csv_tools.check_collector_list(collector_list=self.full_collector_list)

        self.new_collector_list = self.sql_csv_tools.check_collector_list(collector_list=self.new_collector_list,
                                                                          new_agents=True)


    def populate_fields(self, row):
        """populate_fields:
               this populates all the
               initialized data fields per row for input into database,
               make sure to check column list is correct so that the
               row indexes are assigned correctly.
           args:
                row: a row from a botany specimen csv dataframe containing the required columns

        """

        self.barcode = row.CatalogNumber.zfill(9)
        self.raw_barcode = row.CatalogNumber
        self.verbatim_date = row.verbatim_date
        self.start_date = row.start_date
        self.end_date = row.end_date
        self.collector_number = row.collector_number
        self.locality = row.locality
        self.full_name = row.fullname
        self.tax_name = row.taxname
        self.gen_spec = row.gen_spec
        self.qualifier = row.qualifier
        self.name_matched = row.name_matched
        self.genus = row.Genus
        self.family_name = row.Family
        self.is_hybrid = row.Hybrid
        self.author = row.matched_name_author
        self.first_intra = row.first_intra

        self.taxon_id = row.taxon_id

        self.overall_score = row.overall_score

        self.sheet_notes = row.sheet_notes

        self.tax_notes = row.cover_notes

        self.redacted = False

        guid_list = ['collecting_event_guid', 'collection_ob_guid', 'locality_guid', 'determination_guid']
        for guid_string in guid_list:
            setattr(self, guid_string, uuid4())

        self.geography_string = (str(row.County) + ", " + str(row.State) + ", " + str(row.Country)).strip(", ")

        self.GeographyID = self.sql_csv_tools.get_one_match(tab_name='geography', id_col='GeographyID',
                                                            key_col='FullName', match=self.geography_string)


    def populate_taxon(self):
        """populate taxon: creates a taxon list, which checks different rank levels in the taxon,
                         as genus must be uploaded before species , before sub-taxa etc...
                         has cases for hybrid plants, uses regex to separate out sub-taxa hybrids,
                          uses parsed lengths to separate out genus level and species level hybrids.
                        cf. qualifiers already seperated, so less risk of confounding notations.
        """
        self.gen_spec_id = None
        self.taxon_list = []

        if self.is_hybrid is False and self.full_name == "missing taxon in row":
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.family_name)
            # will need to add a condition for project V2 to extract name for order or division
            if not self.taxon_id or pd.isna(self.taxon_id):
                raise ValueError(f"Family {self.family_name} not present in taxon tree")
                # self.taxon_list.append(self.family_name)

        elif not self.is_hybrid and self.full_name != "missing taxon in row":
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.full_name)
        else:
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.full_name,
                                                         taxname=self.tax_name, hybrid=True)
        # append taxon full name
        if not self.taxon_id or pd.isna(self.taxon_id):
            self.taxon_list.append(self.full_name)
            # check base name if base name differs e.g. if var. or subsp.
            if self.full_name != self.first_intra and self.first_intra != self.gen_spec:
                self.first_intra_id = self.sql_csv_tools.taxon_get(name=self.first_intra)
                if not self.first_intra_id or pd.isna(self.first_intra):
                    self.taxon_list.append(self.first_intra)

            if self.full_name != self.gen_spec and self.gen_spec != self.genus:
                self.gen_spec_id = self.sql_csv_tools.taxon_get(name=self.gen_spec)
                # check high taxa gen_spec for author
                self.taxa_author_tnrs(taxon_name=self.gen_spec, barcode=self.barcode)
                # adding base name to taxon_list
                if not self.gen_spec_id or pd.isna(self.gen_spec):
                    self.taxon_list.append(self.gen_spec)

                # base value for gen spec id is set as None so will work either way.
                # checking for genus id
            if self.full_name != self.genus:
                self.genus_id = self.sql_csv_tools.taxon_get(name=self.genus)
                # adding genus name if missing
                if not self.genus_id or pd.isna(self.genus_id):
                    self.taxon_list.append(self.genus)

            self.new_taxa.extend(self.taxon_list)
        else:
            pass

    def generate_taxon_fields(self, index, taxon):
        """generates necessary fields for creating new taxon fields
            args:
                index: index num in the taxon list.
                taxon: the taxon name in the taxon list ,
                       iterrated through from highest to lowest rank"""
        taxon_guid = uuid4()
        rank_name = taxon
        parent_id = self.sql_csv_tools.taxon_get(name=self.parent_list[index + 1])
        if taxon == self.full_name:
            rank_end = self.tax_name
        else:
            rank_end = taxon.split()[-1]

        author_insert = self.author

        if rank_name != self.family_name and rank_name != self.genus:
            tree_item_id, rank_id = self.taxon_assign_defitem(taxon_string=rank_name)
        elif rank_name == self.genus:
            rank_id = 180
            tree_item_id = 12
        else:
            rank_id = 140
            tree_item_id = 11

        self.logger.info(f"{self.full_name}")
        if rank_id < 220 or (taxon == self.full_name and float(self.overall_score) < .90):
            author_insert = ''

        # assigning parent_author if needed , for gen_spec

        if rank_id == 220 and self.full_name != self.gen_spec:
            author_insert = self.parent_author

        if self.is_hybrid is True:
            author_insert = ''

        return author_insert, tree_item_id, rank_end, parent_id, taxon_guid, rank_id


    def create_locality_record(self):
        """create_locality_record:
               defines column and value list , runs them as args
               through create_sql_string and create_table record
               in order to add new locality record to database
        """
        if self.locality == '' or pd.isna(self.locality):
            self.locality=["[unspecified]"]

        table = 'locality'

        column_list = ['TimestampCreated',
                       'TimestampModified',
                       'Version',
                       'GUID',
                       'SrcLatLongUnit',
                       'OriginalLatLongUnit',
                       'LocalityName',
                       'DisciplineID',
                       'GeographyID',
                       'ModifiedByAgentID',
                       'CreatedByAgentID'
                       ]

        value_list = [f'{time_utils.get_pst_time_now_string()}',
                      f'{time_utils.get_pst_time_now_string()}',
                      1,
                      f"{self.locality_guid}",
                      0,
                      0,
                      f"{self.locality}",
                      3,
                      f"{self.GeographyID}",
                      f'{self.created_by_agent}',
                      f'{self.created_by_agent}']

        # removing na values from both lists
        value_list, column_list = remove_two_index(value_list, column_list)

        sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                         val_list=value_list)

        self.sql_csv_tools.insert_table_record(sql=sql)


    def create_agent_id(self):
        """create_agent_id:
                defines column and value list , runs them as
                args through create_sql_string and create_table record
                in order to add new agent record to database.
                Includes a forloop to cycle through multiple collectors.
        """
        table = 'agent'
        for name_dict in self.new_collector_list:
            self.agent_guid = uuid4()

            columns = ['TimestampCreated',
                       'TimestampModified',
                       'Version',
                       'AgentType',
                       'DateOfBirthPrecision',
                       'DateOfDeathPrecision',
                       'FirstName',
                       'LastName',
                       'MiddleInitial',
                       'Title',
                       'DivisionID',
                       'GUID',
                       'ModifiedByAgentID',
                       'CreatedByAgentID']

            values = [f'{time_utils.get_pst_time_now_string()}',
                      f'{time_utils.get_pst_time_now_string()}',
                      1,
                      1,
                      1,
                      1,
                      f"{name_dict['collector_first_name']}",
                      f"{name_dict['collector_last_name']}",
                      f"{name_dict['collector_middle_initial']}",
                      f"{name_dict['collector_title']}",
                      2,
                      f'{self.agent_guid}',
                      f'{self.created_by_agent}',
                      f'{self.created_by_agent}'
                      ]
            # removing na values from both lists
            values, columns = remove_two_index(values, columns)

            sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=columns,
                                                             val_list=values)

            self.sql_csv_tools.insert_table_record(sql=sql)


    def create_collecting_event(self):
        """create_collectingevent:
                defines column and value list , runs them as
                args through create_sql_string and create_table record
                in order to add new collectingevent record to database.
         """

        # re-pulling locality id to reflect update

        self.locality_id = self.sql_csv_tools.get_one_match(tab_name='locality',
                                                            id_col='LocalityID',
                                                            key_col='GUID', match=self.locality_guid)

        table = 'collectingevent'

        column_list = ['TimestampCreated',
                       'TimestampModified',
                       'Version',
                       'GUID',
                       'DisciplineID',
                       'StationFieldNumber',
                       'VerbatimDate',
                       'StartDate',
                       'EndDate',
                       'LocalityID',
                       'ModifiedByAgentID',
                       'CreatedByAgentID'
                       ]

        value_list = [f'{time_utils.get_pst_time_now_string()}',
                      f'{time_utils.get_pst_time_now_string()}',
                      0,
                      f'{self.collecting_event_guid}',
                      3,
                      f'{self.collector_number}',
                      f'{self.verbatim_date}',
                      f'{self.start_date}',
                      f'{self.end_date}',
                      f'{self.locality_id}',
                      f'{self.created_by_agent}',
                      f'{self.created_by_agent}'
                      ]

        # removing na values from both lists
        value_list, column_list = remove_two_index(value_list, column_list)

        sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                         val_list=value_list)

        self.sql_csv_tools.insert_table_record(sql=sql)


    def create_taxon(self):
        """create_taxon: populates the taxon table iteratively by adding higher taxa first,
                         before lower taxa. Assigns taxa ranks and TaxonTreedefItemID.
                         Using parent list in order to populate parent ids, by using the parsed
                         rank levels of each taxon name.
        """
        self.parent_list = [self.full_name, self.first_intra, self.gen_spec, self.genus, self.family_name]
        self.parent_list = unique_ordered_list(self.parent_list)
        for index, taxon in reversed(list(enumerate(self.taxon_list))):
            # getting index pos of taxon in parent list

            author_insert, tree_item_id, rank_end, \
                            parent_id, taxon_guid, rank_id = self.generate_taxon_fields(index=index, taxon=taxon)

            if BotanyImporter.get_is_taxon_id_redacted(conn=self.specify_db_connection, taxon_id=parent_id):
                self.redacted = True

            column_list = ['TimestampCreated',
                           'TimestampModified',
                           'Version',
                           'Author',
                           'FullName',
                           'GUID',
                           'Source',
                           'IsAccepted',
                           'IsHybrid',
                           'Name',
                           'RankID',
                           'TaxonTreeDefID',
                           'ParentID',
                           'ModifiedByAgentID',
                           'CreatedByAgentID',
                           'TaxonTreeDefItemID']

            value_list = [f"{time_utils.get_pst_time_now_string()}",
                          f"{time_utils.get_pst_time_now_string()}",
                          1,
                          author_insert,
                          f"{taxon}",
                          f"{taxon_guid}",
                          "World Checklist of Vascular Plants 2023",
                          True,
                          self.is_hybrid,
                          f"{rank_end}",
                          f"{rank_id}",
                          1,
                          f"{parent_id}",
                          f"{self.created_by_agent}",
                          f"{self.created_by_agent}",
                          f"{tree_item_id}"
                          ]

            value_list, column_list = remove_two_index(value_list, column_list)

            sql = self.sql_csv_tools.create_insert_statement(tab_name="taxon", col_list=column_list,
                                                             val_list=value_list)

            self.sql_csv_tools.insert_table_record(sql=sql)

            logging.info(f"taxon: {taxon} created")


    def create_collection_object(self):
        """create_collection_object:
                defines column and value list , runs them as
                args through create_sql_string and create_table record
                in order to add new collectionobject record to database.
        """
        if self.full_name == "missing taxon in row":
            search_taxon = self.family_name
        else:
            search_taxon = self.full_name
        if self.full_name in self.new_taxa:
            self.taxon_id = self.sql_csv_tools.get_one_match(tab_name='taxon', id_col='TaxonID',
                                                             key_col='FullName', match=search_taxon)


        self.collecting_event_id = self.sql_csv_tools.get_one_match(tab_name='collectingevent',
                                                                    id_col='CollectingEventID',
                                                                    key_col='GUID', match=self.collecting_event_guid)
        if self.redacted is False:
            self.redacted = BotanyImporter.get_is_taxon_id_redacted(conn=self.specify_db_connection,
                                                                    taxon_id=self.taxon_id)

        table = 'collectionobject'

        if self.sheet_notes or self.tax_notes:
            notes = f"{self.sheet_notes + ' ' + self.tax_notes}"
        else:
            notes = ""

        column_list = ['TimestampCreated',
                       'TimestampModified',
                       'CollectingEventID',
                       'Version',
                       'CollectionMemberID',
                       'CatalogNumber',
                       'CatalogedDate',
                       'CatalogedDatePrecision',
                       'GUID',
                       'CollectionID',
                       'Date1Precision',
                       'InventoryDatePrecision',
                       'ModifiedByAgentID',
                       'CreatedByAgentID',
                       'CatalogerID',
                       'Remarks',
                       'ReservedText',
                       'YesNo2'
                       ]

        value_list = [f"{time_utils.get_pst_time_now_string()}",
                      f"{time_utils.get_pst_time_now_string()}",
                      f"{self.collecting_event_id}",
                      0,
                      4,
                      f"{self.barcode}",
                      f"{starting_time_stamp.strftime('%Y-%m-%d')}",
                      1,
                      f"{self.collection_ob_guid}",
                      4,
                      1,
                      1,
                      f"{self.created_by_agent}",
                      f"{self.created_by_agent}",
                      f"{self.created_by_agent}",
                      f"{notes}",
                      f"{self.picturae_config.PROJECT_NAME}",
                      self.redacted]

        # removing na values from both lists
        value_list, column_list = remove_two_index(value_list, column_list)

        sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                         val_list=value_list)

        self.sql_csv_tools.insert_table_record(sql=sql)


    def create_determination(self):
        """create_determination:
                inserts data into determination table, and ties it to collection object table.
           args:
                none
           returns:
                none
        """
        table = 'determination'

        self.collection_ob_id = self.sql_csv_tools.get_one_match(tab_name='collectionobject',
                                                                 id_col='CollectionObjectID',
                                                                 key_col='GUID', match=self.collection_ob_guid)

        if self.taxon_id is not None:

            column_list = ['TimestampCreated',
                           'TimestampModified',
                           'Version',
                           'CollectionMemberID',
                           'DeterminedDatePrecision',
                           'IsCurrent',
                           'Qualifier',
                           'GUID',
                           'TaxonID',
                           'CollectionObjectID',
                           'ModifiedByAgentID',
                           'CreatedByAgentID',
                           'PreferredTaxonID'
                           ]
            value_list = [f"{time_utils.get_pst_time_now_string()}",
                          f"{time_utils.get_pst_time_now_string()}",
                          1,
                          4,
                          1,
                          True,
                          f"{self.qualifier}",
                          f"{self.determination_guid}",
                          f"{self.taxon_id}",
                          f"{self.collection_ob_id}",
                          f"{self.created_by_agent}",
                          f"{self.created_by_agent}",
                          f"{self.taxon_id}"
                          ]

            # removing na values from both lists
            value_list, column_list = remove_two_index(value_list, column_list)

            sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                             val_list=value_list)

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.error(f"failed to add determination , missing taxon for {self.full_name}")


    def create_collector(self):
        """create_collector:
                adds collector to collector table, after
                pulling collection object, agent codes.
           args:
                none
           returns:
                none
        """
        primary_bool = [True, False, False, False, False]
        for index, agent_dict in enumerate(self.full_collector_list):
            table = 'collector'

            agent_id = agent_dict['agent_id']

            if agent_id != '' and pd.notna(agent_id):
                agent_id = agent_dict['agent_id']
            else:
                self.logger.info("new agent pulling agent id")
                agent_id = self.sql_csv_tools.check_agent_name_sql(first_name=agent_dict["collector_first_name"],
                                                              last_name=agent_dict["collector_last_name"],
                                                              middle_initial=agent_dict["collector_middle_initial"],
                                                              title=agent_dict["collector_title"])

            column_list = ['TimestampCreated',
                           'TimestampModified',
                           'Version',
                           'IsPrimary',
                           'OrderNumber',
                           'ModifiedByAgentID',
                           'CreatedByAgentID',
                           'CollectingEventID',
                           'DivisionID',
                           'AgentID']

            value_list = [f"{time_utils.get_pst_time_now_string()}",
                          f"{time_utils.get_pst_time_now_string()}",
                          1,
                          primary_bool[index],
                          1,
                          f"{self.created_by_agent}",
                          f"{self.created_by_agent}",
                          f"{self.collecting_event_id}",
                          2,
                          f"{agent_id}"]

            # removing na values from both lists
            value_list, column_list = remove_two_index(value_list, column_list)

            sql = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                             val_list=value_list)

            self.sql_csv_tools.insert_table_record(sql=sql)


    def hide_unwanted_files(self):
        """hide_unwanted_files:
               function to hide files inside of images folder,
               to filter out images not in images_list.
               Adds a substring '.hidden_' in front of base file name.
           args:
                none
           returns:
                none
        """
        lower_list = [image_path.lower() for image_path in self.image_list]
        folder_paths = set([os.path.dirname(img_path) for img_path in self.image_list])
        for folder_path in folder_paths:
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                if file_path.lower() not in lower_list:
                    new_file_name = f".hidden_{file_name}"
                    new_file_path = os.path.join(folder_path, new_file_name)
                    os.rename(file_path, new_file_path)

    def unhide_files(self):
        """unhide_files:
                Will directly undo the result of hide_unwanted_files.
                Removes substring `.hidden_` from all base filenames.
           args:
                none
           returns:
                none
        """
        folder_paths = set([os.path.dirname(img_path) for img_path in self.image_list])

        for folder_path in folder_paths:
            prefix = ".hidden_"
            for file_name in os.listdir(folder_path):
                if file_name.startswith(prefix):
                    # Calculate the new file name by removing the prefix
                    new_file_name = file_name[len(prefix):]
                    # Construct the full path of the current (hidden) file
                    old_file_path = os.path.join(folder_path, file_name)
                    new_file_path = os.path.join(folder_path, new_file_name)
                    # Rename the file
                    os.rename(old_file_path, new_file_path)

    def upload_records(self):
        """upload_records:
               an ensemble function made up of all row level, and database functions,
               loops through each row of the csv, updates the global values, and creates new table records
           args:
                none
            returns:
                new table records related
        """
        # the order of operations matters, if you change the order certain variables may overwrite

        self.record_full = self.record_full.drop_duplicates(subset=['CatalogNumber'])

        for row in self.record_full.itertuples(index=False):
            if row.CatalogNumber in self.barcode_list:

                self.populate_fields(row)

                # updating barcode present
                self.record_full.loc[self.record_full['CatalogNumber'] == self.raw_barcode, 'barcode_present'] = True

                self.create_agent_list(row)

                if not self.taxon_id or pd.isna(self.taxon_id):
                    self.populate_taxon()
                    self.create_taxon()

                self.create_locality_record()

                if len(self.new_collector_list) > 0:
                    self.create_agent_id()

                self.create_collecting_event()

                self.create_collection_object()

                self.create_determination()

                self.create_collector()
            else:
                pass




    def upload_attachments(self):
        """upload_attachments:
                this function runs Botany importer
                Updates date in
                picturae_config to ensure prefix is
                updated for correct filepath
        """
        self.unhide_files()
        try:
            self.hide_unwanted_files()
            self.botany_importer = BotanyImporter(paths=self.paths, config=self.picturae_config, full_import=True)
            self.unhide_files()
        except Exception as e:
            self.unhide_files()
            self.logger.error(f"{e}")


    def run_all_methods(self):
        """run_all_methods:
                        self-explanatory function, will run all methods in class in sequential manner"""
        # code to create test images for test image uploads
        # setting directory

        # creating backup copy of upload csv before modifying
        copy_path = self.csv_folder + f"PIC_archive{os.path.sep}{os.path.basename(self.file_path)}"

        if not os.path.exists(copy_path):
            shutil.copyfile(self.file_path, copy_path)

        to_current_directory()

        self.assign_col_dtypes()

        # duplicating images with duplicate bar codes
        self.duplicate_images()

        # creating file list after conditions
        self.create_file_list()

        # starting purge timer
        if len(self.barcode_list) >= 1 or len(self.image_list) >= 1:
            self.exit_timestamp()

        # creating tables
        self.upload_records()

        self.record_full.to_csv(self.file_path)

        # creating new taxon list
        if len(self.new_taxa) > 0:
            self.new_taxa = list(set(self.new_taxa))
            self.batch_sql_tools.insert_taxa_added_record(taxon_list=self.new_taxa, df=self.record_full,
                                                          agent_id=self.created_by_agent)
        # uploading attachments

        self.upload_attachments()

        # deleting from main folder after importing images to prevent double uploading

        os.remove(self.file_path)

        if self.picturae_config.MAILING_LIST:
            image_dict = self.botany_importer.image_client.monitoring_dict
            value_list = [len(self.new_taxa)]
            self.image_client.monitoring_tools.send_monitoring_report(subject=f"PIC_Batch{time_utils.get_pst_time_now_string()}",
                                                     time_stamp=starting_time_stamp, image_dict=image_dict,
                                                     value_list=value_list)

        self.logger.info("process finished")
