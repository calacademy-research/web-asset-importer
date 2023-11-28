import logging
import sys
import traceback
import re

from mysql.connector import errorcode
import mysql.connector

class DatabaseInconsistentError(Exception):
    pass


class InvalidFilenameError(Exception):
    pass


class DataInvariantException(Exception):
    pass


class DbUtils:
    def __init__(self, database_user, database_password, database_port, database_host, database_name):
        self.database_user = database_user
        self.database_password = database_password
        self.database_port = database_port
        self.database_host = database_host
        self.database_name = database_name
        self.logger = logging.getLogger('Client.dbutils')
        self.reset = False
        self.cnx = None


    def reset_connection(self):

        self.logger.info(f"Resetting connection to {self.database_host}")
        if self.cnx:
            try:
                self.cnx.close()
            except Exception:
                pass
        self.reset = True
        self.connect()

    def connect(self):

        if self.cnx is None:
            self.logger.debug(f"Connecting to db {self.database_host}...")

            try:
                self.cnx = mysql.connector.connect(user=self.database_user,
                                                   password=self.database_password,
                                                   host=self.database_host,
                                                   port=self.database_port,
                                                   database=self.database_name)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    self.logger.error(f"Starting client...")

                    self.logger.error("SQL: Access denied")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.logger.error("Database does not exist")
                else:
                    self.logger.error(err)
                sys.exit(1)
            except Exception as err:
                self.logger.error(f"Unknown exception: {err}")
                sys.exit(1)

            self.logger.info("Db connected")
        else:
            try:
                self.cnx.ping(reconnect=True, attempts=1, delay=1)
            except Exception as e:
                if self.reset is False:
                    self.logger.warning(f"connection not responsive with Exception as {e}, "
                                        f"attempting to reset connection")
                    self.reset_connection()
                else:
                    self.logger.warning(f"connection not responsive with Exception as {e}. "
                                        f"Max tries attempted, exiting program.")
                    sys.exit(1)
            # self.logger.debug(f"Already connected db {self.database_host}...")



    # added buffered = true so will work properly with forloops
    def get_one_record(self, sql):

        cursor = self.get_cursor(buffered=True)
        try:
            cursor.execute(sql)
            retval = cursor.fetchone()

        except Exception as e:
            print(f"Exception thrown while processing sql: {sql}\n{e}\n", file=sys.stderr, flush=True)
            self.logger.error(traceback.format_exc())

            retval = None
        if retval is None:

            self.logger.warning(f"Warning: No results from: \n\n{sql}\n")
        else:
            retval = retval[0]
        cursor.close()
        return retval

    def get_records(self, query):
        cursor = self.get_cursor()
        attempts = 0
        while attempts <= 3:
            try:
                cursor.execute(query)
                break
            except Exception as e:
                self.logger.error(f"Exception thrown while processing sql: {query}\n{e}\n")
                attempts += 1
        else:
            self.logger.error("get_cursor failed after max attempts")
            sys.exit(1)

        record_list = list(cursor.fetchall())
        self.logger.debug(f"get records SQL: {query}")
        cursor.close()
        return record_list

    def get_cursor(self, buffered=False):
        self.connect()
        attempts = 0
        while attempts <= 3:
            try:
                attempts += 1
                cursor = self.cnx.cursor(buffered=buffered)
                break
            except Exception as e:
                self.logger.error(f"self.cnx.cursor failed with error {e}")
        else:
            self.logger.error("get_records failed after max attempts")
            sys.exit(1)

        return cursor


    def execute(self, sql):
        cursor = self.get_cursor()
        self.logger.debug(f"SQL: {sql}")
        cursor.execute(sql)
        self.cnx.commit()
        cursor.close()

    def commit(self):
        self.cnx.commit()
