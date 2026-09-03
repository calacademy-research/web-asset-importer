import traceback
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import median
from typing import Union, Optional, List, Sequence, Any
import pandas as pd
from specify_db import SpecifyDb
from gen_import_utils import remove_two_index
import time_utils
import string_utils


class DatabaseConnectionError(Exception):
    pass


@dataclass
class SqlStatement:
    sql: str
    params: Optional[Sequence[Any]] = None


class SqlCsvTools:
    def __init__(self, config, logging_level=logging.INFO):
        self.config = config
        self.specify_db_connection = SpecifyDb(db_config_class=self.config)
        self.logger = logging.getLogger(f"Client.{self.__class__.__name__}")
        self.logger.setLevel(logging_level)
        self.check_db_connection()

    def check_db_connection(self):
        """check whether database connection is functional"""
        try:
            self.specify_db_connection.connect()
            self.logger.info("sql_csv_tools connection established")
        except Exception as e:
            raise DatabaseConnectionError from e

    def ensure_db_connection(self):
        """ensure that the database connection is functional. Recreate if exception"""
        try:
            self.specify_db_connection.connect()
            self.logger.info("Database connection established")
        except Exception:
            self.logger.warning("Database connection error. Recreating connection...")
            self.specify_db_connection = SpecifyDb(db_config_class=self.config)
            self.specify_db_connection.connect()
            self.logger.info("Database connection recreated")

    def sql_db_connection(self):
        """standard connector"""
        return self.specify_db_connection.connect()

    def get_record(self, sql, params=None):
        """get one record"""
        return self.specify_db_connection.get_one_record(sql=sql, params=params)

    def get_records(self, sql, params=None):
        """get many records"""
        return self.specify_db_connection.get_records(sql=sql, params=params)

    def get_cursor(self, buffered=False):
        """standard db cursor"""
        return self.specify_db_connection.get_cursor(buffered=buffered)

    def commit(self):
        """Standard db commit"""
        return self.specify_db_connection.commit()

    @staticmethod
    def _normalize_nullable(value):
        """convert empty strings to None for SQL NULL-safe matching"""
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        return value

    def check_agent_name_sql(self, first_name: str, last_name: str, middle_initial: str, title: str):
        """check_agent_name_sql: create a custom sql string, based on number of non-na arguments,
                                queries database for unique agent name.
            args:
                first_name: first name of agent
                last_name: last name of agent
                middle_initial: middle initial of agent
                title: agent's title. (mr, ms, dr. etc..)
        """
        first_name = self._normalize_nullable(first_name)
        last_name = self._normalize_nullable(last_name)
        middle_initial = self._normalize_nullable(middle_initial)
        title = self._normalize_nullable(title)

        sql = """
            SELECT AgentID
            FROM agent
            WHERE
                (FirstName = %s OR (%s IS NULL AND FirstName IS NULL))
                AND (LastName = %s OR (%s IS NULL AND LastName IS NULL))
                AND (MiddleInitial = %s OR (%s IS NULL AND MiddleInitial IS NULL))
                AND (Title = %s OR (%s IS NULL AND Title IS NULL))
        """

        values = [first_name, last_name, middle_initial, title]
        params = tuple(v for value in values for v in (value, value))

        result = self.get_record(sql, params=params)
        return result

    def check_collector_list(self, collector_list, new_agents=False):
        """checks if collector list is empty or contains collector unknown,
           then assigns it unspecified agent dict
           args:
                collector_list: the list of collector name dicts to be processed
                new_agents: if True, list contains new agents to add to database.
                            set to true to avoid re-adding unspecified as an agent id
        """
        agent_id = self.get_one_match("agent", "AgentID", "LastName", "unspecified")

        unknown_dict = {
            "collector_first_name": "",
            "collector_middle_initial": "",
            "collector_last_name": "unspecified",
            "collector_title": "",
            "agent_id": agent_id,
        }

        if not collector_list and not new_agents:
            collector_list.append(unknown_dict)
        else:
            for index, name_dict in enumerate(collector_list):
                no_agent = any(
                    isinstance(value, str) and value.lower() == "collector unknown"
                    for value in name_dict.values()
                )
                if no_agent:
                    collector_list[index] = unknown_dict if not new_agents else None

            collector_list = [x for x in collector_list if x is not None]

        return collector_list

    def get_collecting_event_ids_by_agent_id(self, agent_id: Union[int, str]):
        """
        Return distinct CollectingEventID values for an AgentID from collector.
        """
        sql = """
            SELECT DISTINCT CollectingEventID
            FROM collector
            WHERE AgentID = %s
              AND CollectingEventID IS NOT NULL
        """
        rows = self.get_records(sql, params=(agent_id,))
        return [row[0] for row in rows if row and row[0] is not None]

    def get_agent_collecting_range(self, first_name: str, last_name: str, middle_initial: str, title: str):
        """
        Find an agent via name fields, get their collecting event ids, then return:
        (max_year, min_year, median_year)
        """
        agent_id = self.check_agent_name_sql(
            first_name=first_name,
            last_name=last_name,
            middle_initial=middle_initial,
            title=title,
        )

        if agent_id is None:
            return None, None, None

        collecting_event_ids = self.get_collecting_event_ids_by_agent_id(agent_id)

        if not collecting_event_ids:
            return None, None, None

        placeholders = ", ".join(["%s"] * len(collecting_event_ids))
        sql = f"""
            SELECT YEAR(StartDate) AS StartDateYear
            FROM collectingevent
            WHERE CollectingEventID IN ({placeholders})
              AND StartDate IS NOT NULL
              AND YEAR(StartDate) IS NOT NULL
        """

        rows = self.get_records(sql, params=tuple(collecting_event_ids))
        years = sorted(int(row[0]) for row in rows if row and row[0] is not None)

        if not years:
            return None, None, None

        med = median(years)
        if isinstance(med, float) and med.is_integer():
            med = int(med)

        return max(years), min(years), med

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

            sql = """
                SELECT TaxonID
                FROM taxon
                WHERE LOWER(FullName) LIKE %s
                  AND LOWER(FullName) LIKE %s
                  AND LOWER(FullName) LIKE %s
                  AND LOWER(FullName) LIKE %s
            """

            params = (
                f"%{parts[0].lower()}%",
                f"%{parts[1].lower()}%",
                f"%{parts[2].lower()}%",
                f"%{basename.lower()}%",
            )

            result = self.get_records(sql=sql, params=params)
            return result[0][0] if result and result[0] else None

        elif len(parts) < 3:
            return self.get_one_match(
                tab_name="taxon",
                id_col="TaxonID",
                key_col="FullName",
                match=fullname,
            )

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
        result = self.get_record(sql, params=(match,))
        return result

    def create_insert_statement(self, col_list: list, val_list: list, tab_name: str):
        """create_sql_string:
               creates a new sql insert statement given a list of db columns,
               and values to input.
            args:
                col_list: list of database table columns to fill
                val_list: list of values to input into each table
                tab_name: name of the table you wish to insert data into
        """

        column_list = ", ".join(col_list)
        placeholders = ", ".join(["%s"] * len(val_list))
        sql = f"INSERT INTO {tab_name} ({column_list}) VALUES ({placeholders});"
        return SqlStatement(sql=sql, params=tuple(val_list) if val_list else None)

    def insert_table_record(self, sql, params=None):
        """create_table_record:
                       general code for the inserting of a new record into any table on database,
                       creates connection, and runs sql query. cursor.execute with arg multi, to
                       handle multi-query commands.
                   args:
                       sql: the verbatim sql string, or multi sql query string to send to database
                       params: params to pass to sql command
        """
        cursor = self.get_cursor()
        try:
            if params is not None:
                self.logger.debug(f"running query - {sql} with params {params}")
                cursor.execute(sql, tuple(params))
            else:
                self.logger.debug(f"running query - {sql}")
                cursor.execute(sql)

            self.commit()
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            cursor.close()

    def create_batch_record(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int,
        batch_md5: str,
        agent_id: Union[str, int],
    ):
        """
            create_timestamps:
                uses starting and ending timestamps to create window for sql database purge,
                adds 10 second buffer on either end to allow sql queries to populate.
                appends each timestamp record in picturae_batch table.
            args:
                start_time: starting time stamp
                end_time: ending time stamp
        """
        delt_time = timedelta(seconds=15)
        time_stamp_list = [start_time - delt_time, end_time + delt_time]

        column_list = [
            "batch_MD5",
            "TimestampCreated",
            "TimestampModified",
            "StartTimeStamp",
            "EndTimeStamp",
            "batch_size",
            "CreatedByAgentID",
            "ModifiedByAgentID",
        ]

        value_list = [
            batch_md5,
            time_utils.get_pst_time_now_string(),
            time_utils.get_pst_time_now_string(),
            time_stamp_list[0],
            time_stamp_list[1],
            batch_size,
            agent_id,
            agent_id,
        ]

        value_list, column_list = remove_two_index(value_list, column_list)

        return self.create_insert_statement(
            col_list=column_list,
            val_list=value_list,
            tab_name="picturae_batch",
        )

    def create_update_statement(
        self,
        tab_name,
        agent_id,
        col_list,
        val_list,
        condition_sql,
        condition_params=None,
    ):
        """create_update_string: function used to create sql string used to upload a list of values in the database

            args:
                tab_name: name of table to update
                col_list: list of columns to update
                val_list: list of values with which to update above list of columns(order matters)
                condition_sql: condition sql string used to select sub-sect of records to update.
                condition_params: params for the condition sql
        """
        val_list, col_list = remove_two_index(val_list, col_list)

        update_string = " SET TimestampModified = %s, ModifiedByAgentID = %s"
        params = [time_utils.get_pst_time_now_string(), agent_id]

        for col, val in zip(col_list, val_list):
            update_string += f", {col} = %s"
            params.append(val)

        sql = f"UPDATE {tab_name}{update_string} {condition_sql}"

        if condition_params:
            params.extend(condition_params)

        return SqlStatement(sql=sql, params=tuple(params))

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
                result_id = self.get_one_match(
                    tab_name="taxon",
                    id_col="TaxonID",
                    key_col="FullName",
                    match=name,
                )
                if result_id is None:
                    if "subsp." in name:
                        name = name.replace(" subsp. ", " var. ")
                    elif "var." in name:
                        name = name.replace(" var. ", " subsp. ")

                    result_id = self.get_one_match(
                        tab_name="taxon",
                        id_col="TaxonID",
                        key_col="FullName",
                        match=name,
                    )
            else:
                result_id = self.get_one_match(
                    tab_name="taxon",
                    id_col="TaxonID",
                    key_col="FullName",
                    match=name,
                )

            return result_id

        return self.get_one_hybrid(match=taxname, fullname=name)


    def get_is_taxon_id_redacted(self, taxon_id):
        """
        Retrieve RedactLocality boolean from vtaxon2. Pulls two columns to differentiate between an
        empty response, and a none result from the redacted column.
        """
        sql = "SELECT taxonid, RedactLocality FROM vtaxon2 WHERE taxonid = %s;"
        result = self.get_records(sql, params=(taxon_id,))

        if not result:
            self.logger.info(f"taxon id not yet present in vtaxon2: {taxon_id}")
            return False

        redacted = result[0][1]

        if redacted is None:
            return False

        return redacted is True or redacted == 1 or redacted == b"\x01"