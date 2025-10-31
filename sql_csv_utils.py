import traceback
import pandas as pd
from gen_import_utils import remove_two_index
import time_utils
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
import string_utils
import sys
from specify_db import SpecifyDb
import logging
from typing import Union, Optional, List

class DatabaseConnectionError(Exception):
    pass

@dataclass
class SqlStatement:
    sql: str
    params: Optional[List] = None

class SqlCsvTools:
    def __init__(self, config, logging_level=logging.INFO):
        self.config = config
        self.specify_db_connection = SpecifyDb(db_config_class=self.config)
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)
        self.logger.setLevel(logging_level)
        self.check_db_connection()

    def check_db_connection(self):
        """checking whether database connection is functional"""
        try:
            self.specify_db_connection.connect()
            self.logger.info("sql_csv_tools connection established")
        except Exception as e:
            raise DatabaseConnectionError from e

    def ensure_db_connection(self):
        """Ensure that the database connection is functional. Recreate if an error is raised."""
        try:
            # Attempt to connect to the database
            self.specify_db_connection.connect()
            self.logger.info("Database connection established")
        except Exception as e:
            # If an error is raised, recreate the database connection
            self.logger.warning("Database connection error. Recreating connection...")
            self.specify_db_connection = SpecifyDb(db_config_class=self.config)
            self.specify_db_connection.connect()
            self.logger.info("Database connection recreated")

    def sql_db_connection(self):
        """standard connector"""
        return self.specify_db_connection.connect()

    def get_record(self, sql, params):
        """dbtools get_one_record"""
        return self.specify_db_connection.get_one_record(sql=sql, params=params)

    def get_records(self, sql, params):
        return self.specify_db_connection.get_records(sql=sql, params=params)

    def get_cursor(self, buffered=False):
        """standard db cursor"""
        return self.specify_db_connection.get_cursor(buffered=buffered)

    def commit(self):
        """standard db commit"""
        return self.specify_db_connection.commit()


    # static methods
    def check_agent_name_sql(self, first_name: str, last_name: str, middle_initial: str, title: str):
        """create_name_sql: create a custom sql string, based on number of non-na arguments, the
                            database does not recognize empty strings '' and NA as equivalent.
                            Has conditional to ensure the first statement always starts with WHERE
            args:
                first_name: first name of agent
                last_name: last name of agent
                middle_initial: middle initial of agent
                title: agent's title. (mr, ms, dr. etc..)
        """
        sql = """
                SELECT AgentID FROM agent
                WHERE 
                    (FirstName = %s OR (%s IS NULL AND FirstName IS NULL))
                    AND (LastName = %s OR (%s IS NULL AND LastName IS NULL))
                    AND (MiddleInitial = %s OR (%s IS NULL AND MiddleInitial IS NULL))
                    AND (Title = %s OR (%s IS NULL AND Title IS NULL))
            """
        name_list = [first_name, last_name, middle_initial, title]

        params = tuple(
            item
            for name in name_list
            for item in (lambda processed: (processed, processed))(
                name if name not in (None, "") else None
            )
        )

        result = self.get_record(sql, params=params)
        return result[0] if isinstance(result, (list, dict, set)) else result

    def check_collector_list(self, collector_list, new_agents=False):
        """checks if collector list is empty or contains collector unknown,
           then assigns it unspecified agent dict
           args:
                collector_list: the list of collector name dicts to be processed
                new_agents: if True, list contains new agents to add to database.
                            set to true to avoid re-adding unspecified as an agent id
        """

        sql = "SELECT AgentID FROM agent WHERE LastName = 'unspecified';"
        agent_id = self.specify_db_connection.get_one_record(sql=sql)

        unknown_dict = {
            'collector_first_name': '',
            'collector_middle_initial': '',
            'collector_last_name': 'unspecified',
            'collector_title': '',
            'agent_id': agent_id
        }

        if not collector_list and not new_agents:
            collector_list.append(unknown_dict)
        else:
            for index, name_dict in enumerate(collector_list):
                no_agent = any(isinstance(value, str) and value.lower() == "collector unknown"
                               for value in name_dict.values())
                if no_agent:
                    collector_list[index] = unknown_dict if not new_agents else None
            collector_list = [x for x in collector_list if x is not None]

        return collector_list

    def get_one_hybrid(self, match, fullname):
        """get_one_hybrid:
            used instead of get_one_record for hybrids to
            match multi-term hybrids irrespective of order
            args:
                match = the hybrid term of a taxonomic name e.g Genus A x B,
                        match - "A X B"
                fullname = the full name of the taxonomic name.
        """
        parts = match.split()
        if len(parts) == 3:
            basename = fullname.split()[0]

            sql = '''SELECT TaxonID FROM taxon WHERE 
                             LOWER(FullName) LIKE %s 
                             AND LOWER(FullName) LIKE %s 
                             AND LOWER(FullName) LIKE %s 
                             AND LOWER(FullName) LIKE %s;'''

            # Creating the params tuple with wildcard "%" for LIKE
            params = (f"%{parts[0]}%", f"%{parts[1]}%", f"%{parts[2]}%", f"%{basename}%")

            result = self.get_records(sql=sql, params=params)

            if result:
                taxon_id = result[0]
            else:
                taxon_id = None

            return taxon_id

        elif len(parts) < 3:
            taxon_id = self.get_one_match(tab_name="taxon", id_col="TaxonID", key_col="FullName", match=fullname)

            return taxon_id
        else:
            self.logger.error("hybrid tax name has more than 3 terms")

            return None

    def get_one_match(self, tab_name, id_col, key_col, match):
        """populate_sql:
                creates a custom select statement for get one record,
                from which a result can be gotten more seamlessly
                without having to rewrite the sql variable every time
           args:
                tab_name: the name of the table to select
                id_col: the name of the column in which the unique id is stored
                key_col: column on which to match values
                match: value with which to match key_col
        """
        sql = f"SELECT {id_col} FROM {tab_name} WHERE `{key_col}` = %s;"

        return self.get_record(sql, params=(match,))



    def create_insert_statement(self, col_list: list, val_list: list, tab_name: str):
        """create_sql_string:
               creates a new sql insert statement given a list of db columns,
               and values to input.
            args:
                col_list: list of database table columns to fill
                val_list: list of values to input into each table
                tab_name: name of the table you wish to insert data into
        """
        # removing brackets, making sure comma is not inside of quotations
        column_list = ', '.join(col_list)
        placeholders = ', '.join(['%s'] * len(val_list))
        sql = f'''INSERT INTO {tab_name} ({column_list}) VALUES ({placeholders});'''
        return SqlStatement(sql=sql, params=val_list if val_list else None)

    def insert_table_record(self, sql, params=None):
        """create_table_record:
               general code for the inserting of a new record into any table on database,
               creates connection, and runs sql query. cursor.execute with arg multi, to
               handle multi-query commands.
           args:
               sql: the verbatim sql string, or multi sql query string to send to database
               connection: the connection parameter in the case of specify self.specify_db_connection
               logger: the logger instance of your class self.logger
               sqlite: option for sqlite configuration, as get_cursor()
                          requires database ip, which sqlite does not have
        """
        cursor = self.get_cursor()
        try:
            if params:
                self.logger.debug(f"running query - {sql} with params {params}")
                params = tuple(params)
                cursor.execute(sql, params)
            else:
                self.logger.debug(f"running query - {sql}")
                cursor.execute(sql)
            self.commit()
        except Exception as e:
            self.logger.error(traceback.format_exc())
        cursor.close()

    def create_batch_record(self, start_time: datetime, end_time: datetime,
                            batch_size: int, batch_md5: str, agent_id: Union[str, int]):
        """create_timestamps:
                uses starting and ending timestamps to create window for sql database purge,
                adds 10 second buffer on either end to allow sql queries to populate.
                appends each timestamp record in picturae_batch table.
            args:
                start_time: starting time stamp
                end_time: ending time stamp
        """

        end_time = end_time

        delt_time = timedelta(seconds=15)

        time_stamp_list = [start_time - delt_time, end_time + delt_time]

        column_list = ["batch_MD5",
                       "TimestampCreated",
                       "TimestampModified",
                       "StartTimeStamp",
                       "EndTimeStamp",
                       "batch_size",
                       "CreatedByAgentID",
                       "ModifiedByAgentID"
                       ]
        value_list = [f"{batch_md5}",
                      f"{time_utils.get_pst_time_now_string()}",
                      f"{time_utils.get_pst_time_now_string()}",
                      f"{time_stamp_list[0]}",
                      f"{time_stamp_list[1]}",
                      f"{batch_size}",
                      f"{agent_id}",
                      f"{agent_id}"
                      ]

        value_list, column_list = remove_two_index(value_list, column_list)

        sql_statement = self.create_insert_statement(col_list=column_list, val_list=value_list, tab_name="picturae_batch")

        return sql_statement

    def create_update_statement(self, tab_name, agent_id, col_list, val_list, condition):
        """create_update_string: function used to create sql string used to upload a list of values in the database

            args:
                tab_name: name of table to update
                col_list: list of columns to update
                val_list: list of values with which to update above list of columns(order matters)
                condition: condition sql string used to select sub-sect of records to update.
        """
        val_list, col_list = remove_two_index(val_list, col_list)
        update_string = " SET TimestampModified = %s, ModifiedByAgentID = %s, "
        params = [time_utils.get_pst_time_now_string(), agent_id]

        for col, val in zip(col_list, val_list):
            update_string += f" {col} = %s,"
            params.append(val)

        update_string = update_string.rstrip(',')
        sql = f"UPDATE {tab_name} " + update_string + ' ' + condition
        return SqlStatement(sql=sql, params=params if params else None)

    def taxon_get(self, name, hybrid=False, taxname=None):
        """taxon_get: function to retrieve taxon id from specify database:
            args:
                name: the full taxon name to check
                hybrid: whether the taxon name belongs to a hybrid
                taxname: the name ending substring of a taxon name, only useful for retrieving hybrids.
        """

        name = name.lower()
        if hybrid is False:
            if "subsp." in name or "var." in name:
                result_id = self.get_one_match(tab_name="taxon", id_col="TaxonID", key_col="FullName", match=name)
                if result_id is None:
                    if "subsp." in name:
                        name = name.replace(" subsp. ", " var. ")
                    elif "var." in name:
                        name = name.replace(" var. ", " subsp. ")
                    else:
                        pass

                    result_id = self.get_one_match(tab_name="taxon", id_col="TaxonID", key_col="FullName", match=name)
            else:
                result_id = self.get_one_match(tab_name="taxon", id_col="TaxonID", key_col="FullName", match=name)
            return result_id
        else:
            result_id = self.get_one_hybrid(match=taxname, fullname=name)

            return result_id

    def insert_taxa_added_record(self, taxon_list, df: pd.DataFrame, agent_id: Union[str, int]):
        """new_taxa_record: creates record level data for any new taxa added to the database,
                            populates useful table for qc and troubleshooting
        args:
            taxon_list: list of new taxa added to taxon tree during upload
            connection: connection instance for this sql, using self.specify_db_connection
            df: pandas dataframe, the record table uploaded to the database in question
        """
        taxa_frame = df[df['fullname'].isin(taxon_list)].drop_duplicates(subset=['fullname'])
        for _, row in taxa_frame.iterrows():
            tax_id = self.get_one_match('picturaetaxa_added', 'newtaxID', 'fullname', row['fullname'])
            if tax_id is None:
                sql_statement = self.create_new_tax_tab(row, 'picturaetaxa_added', agent_id)
                self.insert_table_record(sql_statement.sql, sql_statement.params)

    def create_new_tax_tab(self, row, tab_name: str, agent_id: Union[str, int]):
        """create_new_tax: does a similar function as create_unmatch_tab,
                            but instead uploads a table of taxa newly added
                            to the database for QC monitoring(make sure no wonky taxa are added)
            args:
                row: row of new_taxa dataframe through which function will iterate
                df: new_taxa dataframe in order to get column index numbers
                tab_name: name of new_taxa table on mysql database.
        """
        hybrid = string_utils.str_to_bool(row['Hybrid'])

        col_list = ["fullname",
                    "TimestampCreated",
                    "TimestampModified",
                    "batch_MD5",
                    "family",
                    "name",
                    "hybrid",
                    "CreatedByAgentID",
                    "ModifiedByAgentID"]

        val_list = [f"{row['fullname']}",
                    f"{time_utils.get_pst_time_now_string()}",
                    f"{time_utils.get_pst_time_now_string()}",
                    f"{row['batch_md5']}",
                    f"{row['Family']}",
                    f"{row['taxname']}",
                    hybrid,
                    f"{agent_id}",
                    f"{agent_id}"]


        val_list, col_list = remove_two_index(val_list, col_list)

        sql_statement = self.create_insert_statement(col_list, val_list, tab_name)
        return sql_statement