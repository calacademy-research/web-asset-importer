"""This class is for updating individual database fields given a csv"""
from sql_csv_utils import SqlCsvTools
import pandas as pd
import logging
# from uuid import uuid4
# import time_utils
# from gen_import_utils import remove_two_index

class UpdateDbFields:
    def __init__(self, config, csv_path):
        self.logger = logging.getLogger('UpdateDbFields')
        self.config = config
        self.sql_csv_tools = SqlCsvTools(config=self.config)
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




    # def insert_single_agent(self, first, middle, last, title):
    #     self.agent_guid = uuid4()
    #
    #     columns = ['TimestampCreated',
    #                'TimestampModified',
    #                'Version',
    #                'AgentType',
    #                'DateOfBirthPrecision',
    #                'DateOfDeathPrecision',
    #                'FirstName',
    #                'LastName',
    #                'MiddleInitial',
    #                'Title',
    #                'DivisionID',
    #                'GUID',
    #                'ModifiedByAgentID',
    #                'CreatedByAgentID']
    #
    #     values = [f'{time_utils.get_pst_time_now_string()}',
    #               f'{time_utils.get_pst_time_now_string()}',
    #               1,
    #               1,
    #               1,
    #               1,
    #               f"{first}",
    #               f"{last}",
    #               f"{middle}",
    #               f"{title}",
    #               2,
    #               f'{self.agent_guid}',
    #               f'{self.config.AGENT_ID}',
    #               f'{self.config.AGENT_ID}'
    #               ]
    #     # removing na values from both lists
    #     values, columns = remove_two_index(values, columns)
    #
    #     sql = self.sql_csv_tools.create_insert_statement(tab_name='agent', col_list=columns,
    #                                                      val_list=values)
    #
    #     self.sql_csv_tools.insert_table_record(logger_int=self.logger, sql=sql)


    def update_accession(self, barcode, accession):
        """function used to update accession number in the collectionobject table
            args:
                barcode: barcode of the record to update
                accession: the accessions number to update the record with"""

        condition = f"""WHERE CatalogNumber = "{barcode}";"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['AltCatalogNumber'],
                                                         val_list=[accession], condition=condition)


        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)


    def update_herbarium_code(self, barcode, herb_code):
        """function to update herbarium abbreviation code in Modifier column e.g CAS , DS etc...
        args:
            barcode = barcode of record you want to update.
            herb_code = the herbarium acronymn
        """

        condition = f"""WHERE CatalogNumber = "{barcode}";"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['Modifier'],
                                                         val_list=[herb_code], condition=condition)

        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)


    def update_coords(self, barcode, column_list, colname_list):
        """function used to update lat/long columns locality table
            args:
                barcode: the barcode of the record you want ot update
                colname_list: the list of database column names to update with locality info.
                column_list: the list of values to update the locality table with"""

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        condition = f"""WHERE LocalityID = {locality_id};"""


        sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=colname_list,
                                                         val_list=column_list, condition=condition)

        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)
    # def update_utm(self):
    #
    def update_habitat(self, barcode, habitat_string):
        """function used to update habitat string in database
            args:
                barcode: the barcode of the record to update
                habitat_string: the habitat description to update the record with
        """

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        condition = f"""WHERE CollectingEventID = {collecting_event_id}"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='collectingevent', col_list=['Remarks'],
                                                         val_list=[habitat_string], condition=condition)

        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)


    def update_locality_string(self, barcode, loc_string):

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        condition = f"""WHERE LocalityID = {locality_id};"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=['locality'],
                                                         val_list=[loc_string],
                                                         condition=condition
                                                         )
        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)

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

        condition = f"""WHERE LocalityID = {locality_id};"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=['MaxElevation', 'MinElevation',
                                                                                        'OriginalElevationUnit'],
                                                         val_list=[max_elev, min_elev, elev_unit],
                                                         condition=condition
                                                         )
        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)



    def update_TRS(self, barcode, township:str, range:str, section:str):
        """update TRS: updates TRS fields in the database
            args:
                barcode: the barcode of the database record to update.
                township: the township field of TRS.
                range: the range field of TRS.
                section: the section field of TRS."""

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        locality_det_id = self.sql_csv_tools.get_one_match(tab_name='localitydetail',
                                                           id_col='LocalityDetailID',
                                                           key_col='LocalityID', match=locality_id,
                                                           match_type=int)

        condition = f"""WHERE LocalityDetailID = {locality_det_id};"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                         col_list=['Township', 'RangeDesc', 'Section'],
                                                         val_list=[township, range, section], condition=condition)

        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)

    def update_UTM(self, barcode, easting: float, northing: float, datum: str):
        """update_utm: updates utm fields on database
            args:
                barcode: the barcode of the database record to update.
                easting: utm easting
                northing: utm northing
                datum: utm datum"""

        collecting_event_id = self.get_collectingevent_id(barcode=barcode)

        locality_id = self.get_locality_id_with_collectingevent(collecting_event_id=collecting_event_id)

        locality_det_id = self.sql_csv_tools.get_one_match(tab_name='localitydetail',
                                                           id_col='LocalityDetailID',
                                                           key_col='LocalityID', match=locality_id,
                                                           match_type=int)

        condition = f"""WHERE LocalityDetailID = {locality_det_id};"""

        sql = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                         col_list=['UtmEasting', 'UtmNorthing', 'UtmDatum'],
                                                         val_list=[easting, northing, datum], condition=condition)

        self.sql_csv_tools.insert_table_record(sql=sql, logger_int=self.logger)


    # leaving this incomplete/ commented as unsure about final structure of determiner transcribed data

    # def update_determiner(self, barcode, det_first,det_last, det_middle, det_title, det_date):
    #
    #     agent_id = self.sql_csv_tools.check_agent_name_sql(first_name=det_first, last_name=det_last,
    #                                                        middle_initial=det_middle, title=det_title)
    #     if agent_id is None:
    #         self.insert_single_agent(first=det_first, middle=det_middle, last=det_last, title=det_title)
    #
    #         agent_id = self.sql_csv_tools.check_agent_name_sql(first_name=det_first, last_name=det_last,
    #                                                            middle_initial=det_middle, title=det_title)
    #     agent_id




    def process_update_csv(self):
        """master update function, checks for the presence of certain columns in a given csv organized by
            barcode , and then calls the required update functions"""
        # checking accession number
        if "accession_number" in self.update_frame:
            self.update_frame.apply(lambda row: self.update_accession(barcode=row['barcode'],
                                                                      accession=row['accession_number']), axis=1)
        # checking herbarium code / modifier for update
        if 'Modifier' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_herbarium_code(barcode=row['barcode'],
                                                                           herb_code=row['Modifier']), axis=1)
        # checking numeric coordinate values for update
        if 'Longitude1' or 'Latitude2' in self.update_frame:
            up_list = self.make_update_list(check_list=['Longitude1', 'Latitude1', 'Latitude2', 'Longitude2'])

            self.update_frame.apply(lambda row: self.update_coords(barcode=row["barcode"],
                                                                   colname_list=up_list,
                                                                   column_list=row[up_list]),
                                                                   axis=1)
        # checking the string based coordinate values for update
        if 'LatText1' or 'LatText2' or 'Datum' in self.update_frame:
            up_list = self.make_update_list(check_list=['LatText1', 'LongText1', 'LatText2', 'LongText2', 'Datum'])

            self.update_frame.apply(lambda row: self.update_coords(barcode=row["barcode"],
                                                                   colname_list=up_list,
                                                                   column_list=row[up_list]),
                                                                   axis=1)
        # checking the habitat string for update
        if 'Habitat' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_habitat(barcode=row['barcode'],
                                                                    habitat_string=row['Habitat']), axis=1)

        if 'locality' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_locality_string(barcode=row['barcode'],
                                                                            loc_string=row['locality']), axis=1)

        # checking elevation fields for update
        if 'MaxElevation' and 'MinElevation' and 'OriginalElevationUnit' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_elevation(barcode=row["barcode"],
                                                                      max_elev=row['MaxElevation'],
                                                                      min_elev=row['MinElevation'],
                                                                      elev_unit=row['OriginalElevationUnit']), axis=1)
        # checking TRS fields for update
        if 'Township' and 'Range' and 'Section' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_TRS(barcode=row['barcode'],
                                                                township=row['Township'],
                                                                range=row['Range'],
                                                                section=row['Section']), axis=1)
        # checking UTM fields for update
        if 'UtmNorthing' and 'UtmEasting' and 'UtmDatum' in self.update_frame:
            self.update_frame.apply(lambda row: self.update_UTM(barcode=row['barcode'],
                                                                northing=row['UtmNorthing'],
                                                                easting=row['UtmEasting'],
                                                                datum=row['UtmDatum']), axis=1)





