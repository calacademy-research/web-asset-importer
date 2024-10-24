"""test case of the PicturaeImporter class which runs a reduced init and sqlite modified taxon_get function
    for taxon tree testing."""
import logging
import pandas as pd
from uuid import uuid4
from tests.sqlite_csv_utils import SqlLiteTools
from importer import Importer
from picturae_importer import PicturaeImporter
from get_configs import get_config

class AltPicturaeImporterlite(PicturaeImporter):
    def __init__(self, date_string):
        self.picturae_config = get_config(config="Botany_PIC")
        Importer.__init__(self, db_config_class=self.picturae_config, collection_name="Botany")

        self.picdb_config = get_config(config="picbatch")
        self.process_csv_files(date=date_string)
        self.record_full = pd.DataFrame()
        self.init_all_vars()
        self.logger = logging.getLogger("AltPicturaeImporter")
        # keep name as sql_csv_tools since it needs to replace the internally called function
        # sql_csv_tools in the parent class
        self.sql_csv_tools = SqlLiteTools(sql_db="tests/casbotany_lite.db")

    # patched populate_fields function to avoid having to pull from the geography tree, for taxon tree tests
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
        self.sheet_notes = row.sheet_notes
        self.overall_score = row.overall_score
        self.tax_notes = row.cover_notes

        guid_list = ['collecting_event_guid', 'collection_ob_guid', 'locality_guid', 'determination_guid']
        for guid_string in guid_list:
            setattr(self, guid_string, uuid4())

    def populate_fields_without_taxonomy(self, row):

        self.barcode = row.CatalogNumber.zfill(9)
        self.verbatim_date = row.verbatim_date
        self.start_date = row.start_date
        self.end_date = row.end_date
        self.collector_number = row.collector_number
        self.locality = row.locality
        self.sheet_notes = row.sheet_notes
        self.tax_notes = row.cover_notes

        guid_list = ['collecting_event_guid', 'collection_ob_guid', 'locality_guid', 'determination_guid']
        for guid_string in guid_list:
            setattr(self, guid_string, uuid4())


    def process_csv_files(self, date):
        self.date_use = date

        self.csv_folder = self.picturae_config.CSV_FOLDER

        self.file_path = f"PIC_record_99999999_99999999.csv"
