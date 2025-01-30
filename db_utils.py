import logging
import sys
import traceback
from mysql.connector import errorcode
import mysql.connector
import time

class DatabaseInconsistentError(Exception):
    pass


class InvalidFilenameError(Exception):
    pass


class DataInvariantException(Exception):
    pass


class DbUtils:
    def __init__(self, database_user, database_password, database_port, database_host, database_name):
        self.logger = logging.getLogger(f'Client.DbUtils') # must hardcode to see base class name
        self.database_user = database_user
        self.database_password = database_password
        self.database_port = database_port
        self.database_host = database_host
        self.database_name = database_name
        self.cnx = None

    def reset_connection(self):

        self.logger.info(f"Resetting connection to {self.database_host}")
        if self.cnx:
            try:
                self.cnx.close()
            except Exception:
                pass
        self.cnx = None
        self.connect()

    def connect(self):
        if self.cnx is None:
            self.logger.debug(f"Connecting to db {self.database_host}...")

            max_duration = 10800  # 3 hours in seconds
            start_time = time.time()
            delay = 0  # Initial delay
            last_error = None  # Store last encountered error

            while time.time() - start_time < max_duration:
                try:
                    self.cnx = mysql.connector.connect(
                        user=self.database_user,
                        password=self.database_password,
                        host=self.database_host,
                        port=self.database_port,
                        database=self.database_name
                    )
                    self.logger.info("Db connected")
                    return  # Exit function on success

                except mysql.connector.Error as err:
                    last_error = err  # Store the last MySQL error

                    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                        self.logger.error("SQL: Access denied")
                        sys.exit(1)  # No need to retry incorrect credentials

                    elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        self.logger.error("Database does not exist")
                        sys.exit(1)  # No need to retry if database does not exist

                    else:
                        self.logger.error(f"Database connection failed: {err}")

                except Exception as err:
                    last_error = err  # Store the last unknown error
                    self.logger.error(f"Unknown exception: {err}")

                self.logger.debug(f"Retrying in {delay} seconds...")
                time.sleep(delay)

                # Increase the delay by 30 seconds for the next attempt
                delay += 30

            # If we exit the loop, all attempts have failed within 3 hours
            raise RuntimeError(f"Failed to connect to database within 3 hours. Last error: {last_error}")

        else:
            try:
                self.cnx.ping(reconnect=True)
            except Exception as e:
                self.logger.warning(f"Connection not responsive: {e}. Attempting to reset connection.")
                self.reset_connection()

    def get_one_record(self, sql):
        # added buffered = true so will work properly with forloops
        cursor = self.get_cursor(buffered=True)
        try:
            cursor.execute(sql)
            retval = cursor.fetchone()

        except Exception as e:
            print(f"Exception thrown while processing sql: {sql}\n{e}\n", file=sys.stderr, flush=True)
            self.logger.error(traceback.format_exc())

            retval = None
        if retval is None:

            self.logger.info(f"Info: No results from: \n\n{sql}\n")
        else:
            retval = retval[0]
        cursor.close()
        return retval

    def get_records(self, query):
        cursor = self.get_cursor()
        cursor.execute(query)
        record_list = list(cursor.fetchall())
        self.logger.debug(f"get records SQL: {query}")
        cursor.close()
        return record_list

    def get_cursor(self, buffered=False):
        self.connect()
        cursor = self.cnx.cursor(buffered=buffered)
        return cursor

    def execute(self, sql):
        cursor = self.get_cursor()
        self.logger.debug(f"SQL: {sql}")
        cursor.execute(sql)
        self.cnx.commit()
        cursor.close()

    def commit(self):
        self.cnx.commit()
