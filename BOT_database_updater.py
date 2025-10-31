"""This class is for updating individual database fields given a csv"""
from sql_csv_utils import SqlCsvTools
import pandas as pd
import logging
from gen_import_utils import remove_two_index, get_row_value_or_default
import time_utils
from uuid import uuid4

class UpdateBotDbFields:
    def __init__(self, config, date, force_update=False):
        csv_path = f"nfn_csv/{date}/NFN_{date}.csv"
        self.config = config
        self.force_update = force_update
        self.AGENT_ID = config.AGENT_ID
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('UpdateDbFields')
        self.sql_csv_tools = SqlCsvTools(config=self.config, logging_level=self.logger.getEffectiveLevel())
        self.update_frame = pd.read_csv(csv_path)
        self.update_frame.fillna('')
        self.config = config
        self.locality_guid = None
        self.locality_id = None
        self.collecting_event_id = None
        self.process_update_csv()


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
            if (('Longitude1' and 'Latitude1') or ('Longitude2' and 'Latitude2')) and \
                    (('Lat1Text' and 'Long1Text') or ('Lat2Text' and 'Long2Text')) and \
                    'OriginalLatLongUnit' in self.update_frame:
                up_list = self.make_update_list(check_list=['Longitude1', 'Latitude1', 'Longitude2', 'Latitude2',
                                                            'Lat1Text', 'Long1Text', 'Lat2Text', 'Long2Text',
                                                            'OriginalLatLongUnit', 'SrcLatLongUnit', 'Datum'])

                self.update_coords(row=row, colname_list=up_list)

            # checking the habitat string for update
            if 'Habitat' in self.update_frame:
                self.update_habitat(row=row,
                                    habitat_string=row['Habitat'])

            if 'LocalityName' in self.update_frame:
                self.update_locality_string(row=row, loc_string=row['LocalityName'])

            # checking elevation fields for update
            if 'MaxElevation' and 'MinElevation' and 'OriginalElevationUnit' in self.update_frame:
                up_list = self.make_update_list(check_list=['MaxElevation', 'MinElevation', 'OriginalElevationUnit'])

                self.update_elevation(row=row, colname_list=up_list,
                                      val_list=row[up_list]
                                      )

            # updating/creating localitydetail table record, column checks done inside function
            if "UtmNorthing" or "UtmEasting" or "Township" or "Range" or "Section" in self.update_frame:
                self.update_locality_det(row=row)

            self.locality_guid = None
            self.locality_id = None
            self.collecting_event_id = None


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
                                                               key_col='CatalogNumber', match=barcode)
        return collecting_event_id

    def get_locality_id_with_collectingevent(self, collecting_event_id):
        """get locality id with collectingevent: gets locality id with collecting event id
            args:
                collecting_event_id: barcode of the collecting_event_id
        """

        locality_id = self.sql_csv_tools.get_one_match(tab_name='collectingevent',
                                                       id_col='LocalityID',
                                                       key_col='CollectingEventID', match=collecting_event_id)
        return locality_id


    def update_accession(self, barcode, accession):
        """function used to update accession number in the collectionobject table
            args:
                barcode: barcode of the record to update
                accession: the accessions number to update the record with"""

        is_present = self.sql_csv_tools.get_one_match(tab_name='collectionobject', id_col="AltCatalogNumber",
                                                      key_col="CatalogNumber",
                                                      match=f"{barcode}")

        if pd.isna(is_present) or self.force_update:
            condition = f"""WHERE CatalogNumber = {barcode};"""

            sql_statement = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['AltCatalogNumber'],
                                                                     val_list=[accession], condition=condition,
                                                                     agent_id=self.AGENT_ID)

            self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)
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

            sql_statement = self.sql_csv_tools.create_update_statement(tab_name='collectionobject', col_list=['Modifier'],
                                                                     val_list=[herb_code], condition=condition,
                                                                     agent_id=self.AGENT_ID)

            self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)
        else:
            self.logger.info(f"Herbarium code already in collectionobject table at:{barcode}")


    def update_collectingevent_locality(self, row):
        """function used to update lat/long columns locality table
            args:
                barcode: the barcode of the record you want ot update
                colname_list: the list of database column names to update with locality info.
                column_list: the list of values to update the locality table with
        """
        if self.locality_guid is None:
            if pd.notna(row['barcode']):
                self.collecting_event_id = self.get_collectingevent_id(barcode=row['barcode'])

            else:
                self.collecting_event_id = row["CollectingEventID"]

            locality_id = self.sql_csv_tools.get_one_match(tab_name="collectingevent", id_col="LocalityID",
                                                           key_col="CollectingEventID",
                                                           match=self.collecting_event_id)

            if self.collecting_event_id:

                count = self.check_key_unique(tab="collectingevent", id_col="LocalityID", id=locality_id,
                                              primarykey="CollectingEventID")

                if count == 1:
                    self.locality_id = locality_id
                    self.locality_guid = self.sql_csv_tools.get_one_match(tab_name='locality', id_col="GUID",
                                                                          key_col="LocalityID", match=self.locality_id)
                    self.logger.info("locality id unique, editing locality")
                else:

                    self.locality_guid = self.create_new_locality_record(row)


                    self.locality_id = self.sql_csv_tools.get_one_match(tab_name='locality', id_col="LocalityID",
                                                                        key_col="GUID", match=self.locality_guid)

                    self.logger.info(f"new locality being created at {self.locality_id} for collectingevent")

                    condition = f"""WHERE CollectingEventID = {self.collecting_event_id}"""

                    sql_statement = self.sql_csv_tools.create_update_statement(tab_name='collectingevent', col_list=['LocalityID'],
                                                                             val_list=[self.locality_id], condition=condition,
                                                                             agent_id=self.AGENT_ID)

                    self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)
        else:
            pass


    def update_coords(self, colname_list, row):

        val_list = row[colname_list].copy()

        if "Latitude2" in colname_list:
            self.latlongtype = "Line"
        else:
            self.latlongtype = "Point"

        val_list["LatLongType"] = self.latlongtype

        if "LatLongType" not in colname_list:
            colname_list.append("LatLongType")



        self.update_collectingevent_locality(row=row)

        condition = f"""WHERE LocalityID = '{self.locality_id}';"""

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=colname_list,
                                                                 val_list=val_list, condition=condition,
                                                                 agent_id=self.AGENT_ID)

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)




    def update_habitat(self, row, habitat_string):
        """function used to update habitat string in database
            args:
                barcode: the barcode of the record to update
                habitat_string: the habitat description to update the record with
        """

        self.update_collectingevent_locality(row=row)


        condition = f"""WHERE CollectingEventID = {self.collecting_event_id}"""

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='collectingevent', col_list=['Remarks'],
                                                                 val_list=[habitat_string], condition=condition,
                                                                 agent_id=self.AGENT_ID)

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)


    def update_locality_string(self, row, loc_string):
        """update_locality_string: """

        self.update_collectingevent_locality(row=row)

        condition = f"""WHERE LocalityID = {self.locality_id};"""

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=['LocalityName'],
                                                                 val_list=[loc_string],
                                                                 condition=condition,
                                                                 agent_id=self.AGENT_ID
                                                                )

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)

    def check_key_unique(self, tab, id_col, id, primarykey):

        sql = f'''SELECT COUNT(DISTINCT %s) from %s WHERE %s = %s'''


        params = (primarykey, tab, id_col, id, )

        count = self.sql_csv_tools.get_record(sql=sql, params=params)

        if not pd.isna(count):
            count = int(count)

            return count
        else:
            return 0
    def update_elevation(self, row, colname_list, val_list):
        """updates the elevation fields in the locality table, assumes having at least min max and unit
            note: according to NfN we won't be parsing elevation accuracy
            args:
                barcode: the barcode of the record you want to update the locality of
                max_elev: the maximum elevation in float or int format
                min_elev: the minimum elevation in float or int format
                elev_unit: ft. for feet or m for meters
        """
        self.update_collectingevent_locality(row=row)


        condition = f"""WHERE LocalityID = {self.locality_id};"""

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='locality', col_list=colname_list,
                                                                 val_list=val_list,
                                                                 condition=condition,
                                                                 agent_id=self.AGENT_ID
                                                                 )

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)



    def create_new_locality_record(self, row):
        """create_locality_record:
               defines column and value list , runs them as args
               through create_sql_string and create_table record
               in order to add new locality record to database
        """
        locality_guid = uuid4()
        table = 'locality'

        geography_id = self.sql_csv_tools.get_one_match(tab_name="locality", id_col="GeographyID",
                                                        key_col="LocalityName",
                                                        match=row['LocalityName'])

        column_list = ['TimestampCreated',
                       'TimestampModified',
                       'Version',
                       'GUID',
                       'SrcLatLongUnit',
                       'OriginalLatLongUnit',
                       'LocalityName',
                       'Text2',
                       'DisciplineID',
                       'GeographyID',
                       'ModifiedByAgentID',
                       'CreatedByAgentID'
                       ]

        value_list = [f'{time_utils.get_pst_time_now_string()}',
                      f'{time_utils.get_pst_time_now_string()}',
                      1,
                      f"{locality_guid}",
                      0,
                      0,
                      f"{row['LocalityName']}",
                      f"{row['Text2']}",
                      3,
                      f"{geography_id}",
                      f"{self.config.AGENT_ID}",
                      f"{self.config.AGENT_ID}"]

        # removing na values from both lists
        value_list, column_list = remove_two_index(value_list, column_list)

        sql_statement = self.sql_csv_tools.create_insert_statement(tab_name=table, col_list=column_list,
                                                                   val_list=value_list)

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)

        return locality_guid



    def create_locality_detail_tab(self, row):
        """create_locality_detail_tab: most specimens will not have a locality details table record to update,
           so one must be created instead"""


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
                      f"{self.locality_id}"
                      ]



        values, columns = remove_two_index(value_list=value_list, column_list=column_list)

        sql_statement = self.sql_csv_tools.create_insert_statement(val_list=values, col_list=columns,
                                                         tab_name="localitydetail")

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)

        self.logger.info("New entry created in the localitydetail table")


    def update_locality_det(self, row):

        """update_locality_det:
                creates localitydetail record if not exists, if exists, updates UTM and TRS fields if present
            args:
            row: row of update csv to process"""
        self.locality_det_id = self.sql_csv_tools.get_one_match(tab_name="localitydetail", id_col="LocalityDetailID",
                                                                key_col="LocalityID", match=self.locality_id)

        if self.locality_det_id is None:
            self.create_locality_detail_tab(row=row)

            self.locality_det_id = self.sql_csv_tools.get_one_match(tab_name="localitydetail",
                                                                    id_col="LocalityDetailID",
                                                                    key_col="LocalityID", match=self.locality_id)
        else:
            self.logger.info("editing existing localitydetail entry")

            if 'Township' or 'Range' or 'Section' in self.update_frame:

                self.update_trs(row=row)

            else:
                self.logger.info(f"No TRS Fields in data, skipping update")

            if 'UtmNorthing' or 'UtmEasting' or 'UtmDatum' or 'UtmZone' in self.update_frame:

                self.update_utm(row=row)

            else:
                self.logger.info(f"No UTM fields in data, skipping update")


    def update_trs(self, row):
        """update_trs: updates TRS fields on database table localitydetail
            args:
                locality_det_id: the localitydetail ID to update.
                row: row from update csv"""

        condition = f"""WHERE LocalityDetailID = {self.locality_det_id};"""

        col_list = self.make_update_list(['Township', 'RangeDesc', 'Section'])

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                         col_list=col_list,
                                                         val_list=row[col_list],
                                                         condition=condition,
                                                         agent_id=self.AGENT_ID)

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)


    def update_utm(self, row):
        """update_utm: updates utm fields on database table localitydetail
            args:
                locality_det_id: the localitydetail ID to update.
                row: row from update csv"""

        col_list = self.make_update_list(['UtmEasting', 'UtmNorthing', 'UtmDatum', 'UtmZone'])

        condition = f"""WHERE LocalityDetailID = {self.locality_det_id};"""

        sql_statement = self.sql_csv_tools.create_update_statement(tab_name='localitydetail',
                                                                 col_list=col_list,
                                                                 val_list=row[col_list],
                                                                 condition=condition,
                                                                 agent_id=self.AGENT_ID)

        self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)


# def update_dummy():
#     from get_configs import get_config
#     config = get_config(config='Botany')
#     UpdateBotDbFields(config=config, date="2024-02-28", force_update=True)
#
# update_dummy()