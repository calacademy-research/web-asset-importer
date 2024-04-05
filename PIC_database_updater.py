"""This class is for updating individual database fields given a csv"""
from sql_csv_utils import SqlCsvTools
import pandas as pd
import logging
from gen_import_utils import remove_two_index, get_row_value_or_default
import time_utils


class UpdateDbFields:
    def __init__(self, config, date, force_update=False):

        csv_path = f"nfn_csv/{date}/NFN_{date}.csv"
        self.config = config
        self.force_update = force_update
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)
        self.sql_csv_tools = SqlCsvTools(config=self.config, logging_level=self.logger.getEffectiveLevel())
        self.update_frame = pd.read_csv(csv_path)
        self.config = config
        self.process_update_csv()

    def make_update_list(self, check_list):
        """for tables that may require multiple columns to be updated,
            checks csv for presence,
            and appends to variable length column list"""
        up_list = []
        for field in check_list:
            if field in self.update_frame:
                up_list.append(field)
        return up_list

    def get_collectingevent_id(self, barcode):
        """get collecting event id: gets collecting event id with sql connection
        args:
            barcode: barcode of the record with which to match collecting event id"""

        collecting_event_id = self.sql_csv_tools.get_one_match(tab_name='collectionobject',
                                                               id_col='CollectingEventID',
                                                               key_col='CatalogNumber', match=barcode,
                                                               match_type=int)
        return collecting_event_id

    def get_locality_id_with_collectingevent(self, collecting_event_id):
        """get locality id with collectingevent: gets locality id with collecting event id
            args:
                collecting_event_id: barcode of the collecting_event_id
        """

        locality_id = self.sql_csv_tools.get_one_match(tab_name='collectingevent',
                                                       id_col='LocalityID',
                                                       key_col='CollectingEventID', match=collecting_event_id,
                                                       match_type=int)
        return locality_id


    def update_accession(self, barcode, accession):
        """function used to update accession number in the collectionobject table
            args:
                barcode: barcode of the record to update
                accession: the accessions number to update the record with"""

        is_present = self.sql_csv_tools.get_one_match(tab_name='collectionobject', id_col="AltCatalogNumber",
                                                      key_col="CatalogNumber",
                                                      match=f"{barcode}", match_type=int)

        if pd.isna(is_present) or self.force_update:
            condition = f"""WHERE CatalogNumber = {barcode};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['AltCatalogNumber'],
                                                             val_list=[accession], condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)
        else:
            self.logger.info(f"Accession number already in collectionobject table at: {barcode}")

    def update_herbarium_code(self, barcode, herb_code):
        """function to update herbarium abbreviation code in Modifier column e.g CAS , DS etc...
        args:
            barcode = barcode of record you want to update.
            herb_code = the herbarium acronymn
        """
        is_present = self.sql_csv_tools.get_one_match(tab_name="collectionobject", id_col="Modifier",
                                                      key_col="CatalogNumber", match=f"{barcode}")
        if pd.isna(is_present) or self.force_update:

            condition = f"""WHERE CatalogNumber = {barcode};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['Modifier'],
                                                             val_list=[herb_code], condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)
        else:
            self.logger.info(f"Herbarium code already in collectionobject table at:{barcode}")


    def update_coords(self, barcode, val_list, colname_list):
        """function used to update lat/long columns locality table
            args:
                barcode: the barcode of the record you want ot update
                colname_list: the list of database column names to update with locality info.
                column_list: the list of values to update the locality table with
        """

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        # not comprehensive check of every coordinate field,
        # but if assuming coordinates will all come together just checking one should be ok
        is_present = self.sql_csv_tools.get_one_match(tab_name="locality", id_col="Latitude1", key_col="LocalityID",
                                                      match=f"{locality_id}")

        if pd.isna(is_present) or self.force_update:

            condition = f"""WHERE LocalityID = {locality_id};"""


            sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=colname_list,
                                                             val_list=val_list, condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"Lat/Long already present in locality table at: {locality_id}")


    def update_habitat(self, barcode, habitat_string):
        """function used to update habitat string in database
            args:
                barcode: the barcode of the record to update
                habitat_string: the habitat description to update the record with
        """

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        is_present = self.sql_csv_tools.get_one_match(tab_name='collectingevent', id_col="Remarks",
                                                      key_col="CollectingEventID", match=f"{collecting_event_id}",
                                                      match_type=int)

        if is_present or self.force_update:

            condition = f"""WHERE CollectingEventID = {collecting_event_id}"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='collectingevent', col_list=['Remarks'],
                                                             val_list=[habitat_string], condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"Remarks already filled in collectingevent table at: {collecting_event_id}")


    def update_locality_string(self, barcode, loc_string):
        """update_locality_string: """

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        is_present = self.sql_csv_tools.get_one_match(tab_name='locality', id_col="LocalityName",
                                                      key_col="LocalityID", match=f"{locality_id}",
                                                      match_type=int)

        if pd.isna(is_present) or self.force_update:

            condition = f"""WHERE LocalityID = {locality_id};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=['LocalityName'],
                                                             val_list=[loc_string],
                                                             condition=condition
                                                             )

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"Locality string already in locality table at:{locality_id}")


    def update_elevation(self, barcode, max_elev, min_elev, elev_unit):
        """updates the elevation fields in the locality table, assumes having at least min max and unit
            note: according to NfN we won't be parsing elevation accuracy
            args:
                barcode: the barcode of the record you want to update the locality of
                max_elev: the maximum elevation in float or int format
                min_elev: the minimum elevation in float or int format
                elev_unit: ft. for feet or m for meters
        """
        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        is_present = self.sql_csv_tools.get_one_match(tab_name="locality", id_col="MaxElevation", key_col="LocalityID",
                                                      match=f"{locality_id}", match_type=int)

        if pd.isna(is_present) or self.force_update:

            condition = f"""WHERE LocalityID = {locality_id};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=['MaxElevation', 
                                                                                            'MinElevation',
                                                                                            'OriginalElevationUnit'],
                                                             val_list=[max_elev, min_elev, elev_unit],
                                                             condition=condition
                                                             )

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"Elevation already in locality table at: {locality_id}")

    def create_locality_detail_tab(self, row):
        """create_locality_detail_tab: most specimens will not have a locality details table record to update,
           so one must be created instead"""

        collecting_event_id = self.get_collectingevent_id(barcode=row['barcode'])

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        if "UtmNorthing" or "UtmEasting" or "Township" or "Range" or "Section" in self.update_frame:

            column_list = ['TimestampCreated',
                           'TimestampModified',
                           'Version',
                           'RangeDesc',
                           'Section',
                           'Township',
                           'UtmDatum',
                           'UtmEasting',
                           'UtmNorthing',
                           'UtmZone',
                           'CreatedByAgentID',
                           'ModifiedByAgentID',
                           'LocalityID'
                           ]

            value_list = [f'{time_utils.get_pst_time_now_string()}',
                          f'{time_utils.get_pst_time_now_string()}',
                          0,
                          f"{get_row_value_or_default(row=row, column_name='Range')}",
                          f"{get_row_value_or_default(row=row, column_name='Section')}",
                          f"{get_row_value_or_default(row=row, column_name='Township')}",
                          f"{get_row_value_or_default(row=row, column_name='UtmDatum')}",
                          f"{get_row_value_or_default(row=row, column_name='UtmEasting')}",
                          f"{get_row_value_or_default(row=row, column_name='UtmNorthing')}",
                          f"{get_row_value_or_default(row=row, column_name='UtmZone')}",
                          f"{self.config.AGENT_ID}",
                          f"{self.config.AGENT_ID}",
                          f"{locality_id}"
                          ]

            values, columns = remove_two_index(value_list=value_list, column_list=column_list)

            sql = self.sql_csv_tools.create_insert_statement(val_list=values, col_list=columns, 
                                                             tab_name="localitydetail")

            self.sql_csv_tools.insert_table_record(sql=sql)


    def update_locality_det(self, row):

        """update_locality_det:
                creates localitydetail record if not exists, if exists, updates UTM and TRS fields if present
            args:
            row: row of update csv to process"""

        collecting_event_id = self.get_collectingevent_id(barcode=row['barcode'])

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        locality_det_id = self.sql_csv_tools.get_one_match(tab_name='localitydetail',
                                                           id_col='LocalityDetailID',
                                                           key_col='LocalityID', match=locality_id,
                                                           match_type=int)

        if not locality_det_id:
            self.create_locality_detail_tab(row=row)
            self.logger.info("New entry created in the localitydetail table")

        else:
            if 'Township' and 'Range' and 'Section' in self.update_frame:

                self.update_trs(locality_det_id=locality_det_id, row=row)

            else:
                self.logger.info(f"No TRS Fields in data, skipping update")

            if 'UtmNorthing' and 'UtmEasting' and 'UtmDatum' in self.update_frame:

                self.update_utm(locality_det_id=locality_det_id, row=row)

            else:
                self.logger.info(f"No UTM fields in data, skipping update")

    def update_trs(self, locality_det_id, row):
        """update_trs: updates TRS fields on database table localitydetail
            args:
                locality_det_id: the localitydetail ID to update.
                row: row from update csv"""

        is_present = self.sql_csv_tools.get_one_match(tab_name="localitydetail", id_col="Township",
                                                      key_col="LocalityDetailID",
                                                      match=f"{locality_det_id}", match_type=int)

        if is_present or self.force_update:

            condition = f"""WHERE LocalityDetailID = {locality_det_id};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                             col_list=['Township', 'RangeDesc', 'Section'],
                                                             val_list=[row['Township'], row['Range'],
                                                                       row['Section']],
                                                             condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"TRS already present in localitydetail table at: {locality_det_id}")


    def update_utm(self, locality_det_id, row):
        """update_utm: updates utm fields on database table localitydetail
            args:
                locality_det_id: the localitydetail ID to update.
                row: row from update csv"""

        is_present = self.sql_csv_tools.get_one_match(tab_name="localitydetail", id_col="UtmEasting",
                                                      key_col="LocalityDetailID",
                                                      match=f"{locality_det_id}", match_type=int)

        if is_present or self.force_update:

            condition = f"""WHERE LocalityDetailID = {locality_det_id};"""

            sql = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                             col_list=['UtmEasting', 'UtmNorthing', 'UtmDatum'],
                                                             val_list=[row['UtmEasting'], row['UtmNorthing'],
                                                                       row['UtmDatum']],
                                                             condition=condition)

            self.sql_csv_tools.insert_table_record(sql=sql)

        else:
            self.logger.info(f"UTM already present in localitydetail table at: {locality_det_id}")


    def process_update_csv(self):
        """master update function, checks for the presence of certain columns in a given csv organized by
            barcode , and then calls the required update functions"""
        # checking accession number
        for index, row in self.update_frame.iterrows():
            if "accession_number" in self.update_frame:
                self.update_accession(barcode=row['barcode'], accession=row['accession_number'])
            # checking herbarium code / modifier for update
            if "Modifier" in self.update_frame:
                self.update_herbarium_code(barcode=row['barcode'], herb_code=row['Modifier'])

            # checking lat/long values for update
            if 'Longitude1' or 'Latitude2' or 'LatText1' or 'LatText2' or 'Datum' in self.update_frame:
                up_list = self.make_update_list(check_list=['Longitude1', 'Latitude1', 'Longitude2', 'Latitude2',
                                                            'Lat1Text', 'Long1Text', 'Lat2Text', 'Long2Text', 'Datum'])

                self.update_coords(barcode=row['barcode'],  colname_list=up_list, val_list=row[up_list])

            # checking the habitat string for update
            if 'Habitat' in self.update_frame:
                self.update_habitat(barcode=row['barcode'],
                                    habitat_string=row['Habitat'])

            if 'LocalityName' in self.update_frame:
                self.update_locality_string(barcode=row['barcode'], loc_string=row['LocalityName'])

            # checking elevation fields for update
            if 'MaxElevation' and 'MinElevation' and 'OriginalElevationUnit' in self.update_frame:
                self.update_elevation(barcode=row['barcode'],
                                      max_elev=row['MaxElevation'],
                                      min_elev=row['MinElevation'],
                                      elev_unit=row['OriginalElevationUnit'])

            # updating/creating localitydetail table record, column checks done inside function
            self.update_locality_det(row=row)
