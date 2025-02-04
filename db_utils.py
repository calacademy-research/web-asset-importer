import logging
import traceback
from mysql.connector import errorcode
import mysql.connector
import time
import settings

class DatabaseInconsistentError(Exception):
    pass


class InvalidFilenameError(Exception):
    pass


class DataInvariantException(Exception):
    pass

class DbUtils:
    def __init__(self, database_user, database_password, database_port, database_host, database_name):
        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')
        self.database_user = database_user
        self.database_password = database_password
        self.database_port = database_port
        self.database_host = database_host
        self.database_name = database_name
        self.cnx = None

    @staticmethod
    def retry_with_backoff(max_duration=10800, initial_delay=0):
        """Decorator to retry a function with exponential backoff."""

        def decorator(func):
            def wrapper(*args, **kwargs):
                logger = args[0].logger
                start_time = time.time()
                delay = initial_delay
                last_error = None

                while time.time() - start_time < max_duration:
                    try:
                        return func(*args, **kwargs)  # Attempt to execute the function
                    except Exception as ex:
                        last_error = ex
                        logger.error(f"Error in {func.__name__}: {ex}")

                    logger.info(f"Retrying {func.__name__} in {delay} seconds...")
                    time.sleep(delay)
                    delay += delay + 30  # increasing backoff

                logging.error(f"Failed to execute {func.__name__} within {max_duration} seconds. Last error: {last_error}")
                return None  # Return None if all retries fail

            return wrapper

        return decorator

    @retry_with_backoff()
    def connect(self):
        """Attempts to establish a database connection with logging but without redundant retry logic."""

        if self.cnx and self.cnx.is_connected():
            return True  # Connection is already established

        self.logger.debug(f"Connecting to db {self.database_host}...")

        try:
            self.cnx = mysql.connector.connect(
                user=self.database_user,
                password=self.database_password,
                host=self.database_host,
                port=self.database_port,
                database=self.database_name
            )
            self.logger.info("Db connected")
            return True  # Successfully connected

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.logger.error(f"SQL: Access denied. Host: {settings.SQL_HOST}, User: {settings.SQL_USER}")
                return False
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.error("Database does not exist")
                return False
            else:
                self.logger.error(f"Database connection failed: {err}")
                raise

        except Exception as err:
            self.logger.error(f"Unknown exception during connection: {err}")
            raise

    @retry_with_backoff()
    def get_one_record(self, sql):
        """Fetches a single record from the database and returns the first column value or None if no result is found."""

        cursor = None
        retval = None

        try:
            cursor = self.get_cursor(buffered=True)
            cursor.execute(sql)
            row = cursor.fetchone()  # Fetch one record
            retval = row[0] if row else None  # Avoids NoneType indexing error

        except mysql.connector.Error as err:
            self.logger.error(f"SQL error while processing query: {sql}\n{err}\n")
            self.logger.error(traceback.format_exc())

            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.logger.error(f"Access denied: User: {self.database_user}, Host: {self.database_host}")
                retval = None  # Return None since access issues are non-retryable

            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.logger.error("Database does not exist")
                retval = None  # Non-retryable

            elif err.errno >= 500:
                self.logger.error("Retryable MySQL error occurred")
                raise  # Allow retry mechanism to handle it

            else:
                retval = None  # Non-retryable errors return None

        except Exception as err:
            self.logger.error(f"Unexpected exception during query execution: {err}")
            self.logger.error(traceback.format_exc())
            raise  # Raise for unknown exceptions

        finally:
            if cursor:
                cursor.close()  # Ensure cursor is always closed

        if retval is None:
            self.logger.info(f"Info: No results from query:\n\n{sql}\n")

        return retval

    @retry_with_backoff()
    def get_records(self, query):
        cursor = self.get_cursor()
        cursor.execute(query)
        record_list = list(cursor.fetchall())
        self.logger.debug(f"get records SQL: {query}")
        cursor.close()
        return record_list

    def reset_connection(self):
        """Closes the connection and reconnects."""
        self.logger.info(f"Resetting connection to {settings.SQL_HOST}")

        if self.cnx:
            try:
                self.cnx.close()
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")

        self.connect()  # This will automatically retry with @retry_with_backoff

    def get_cursor(self, buffered=False):
        """Gets a database cursor, ensuring connection is available."""
        try:
            if self.cnx is None or not self.cnx.is_connected():
                self.connect()  # Let retry_with_backoff handle reconnection
            return self.cnx.cursor(buffered=buffered)
        except mysql.connector.OperationalError:
            self.logger.error("Failed to connect, resetting DB connection")
            self.reset_connection()
            return self.cnx.cursor(buffered=buffered)  # Retry getting the cursor


    @retry_with_backoff()
    def execute(self, sql):
        """Executes a SQL statement with logging and connection management."""

        self.logger.debug(f"SQL: {sql}")

        try:
            if self.cnx is None or not self.cnx.is_connected():
                self.connect()

            cursor = self.cnx.cursor(buffered=True)  # Use buffered cursor
            cursor.execute(sql)
            self.cnx.commit()
            cursor.close()
            return True  # Execution successful

        except mysql.connector.Error as err:
            self.logger.error(f"SQL execution failed: {err}")
            if err.errno in {errorcode.CR_SERVER_GONE_ERROR, errorcode.CR_SERVER_LOST}:
                self.reset_connection()  # Reset on connection loss

        except Exception as ex:
            self.logger.error(f"Unknown error during SQL execution: {ex}")
            self.reset_connection()  # Reset and try again

        return False  # Return False on failure


    def commit(self):
        self.cnx.commit()
