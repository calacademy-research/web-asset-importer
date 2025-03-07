import sqlite3
from sql_csv_utils import SqlCsvTools
from get_configs import get_config
import traceback
import sys
import re
class SqlLiteTools(SqlCsvTools):
    def __init__(self, sql_db):
        """Initialize the SQLite version of SqlCsvTools"""
        picturae_config = get_config(config="Botany_PIC")
        super().__init__(config=picturae_config)
        self.sqlite_db = sql_db
        self.connection = self.sql_db_connection()
        self.specify_db_connection.cnx = self.sql_db_connection()
        self.specify_db_connection.get_cursor = self.get_cursor

    def sql_db_connection(self):
        """Return an SQLite database connection"""
        return sqlite3.connect(database=self.sqlite_db)

    def get_cursor(self, buffered=True):
        """Return a SQLite cursor"""
        return self.connection.cursor()

    def get_record(self, sql, params):
        sql = self.convert_sql(sql)
        return self.specify_db_connection.get_one_record(sql=sql, params=params)

    def get_records(self, sql, params):
        sql = self.convert_sql(sql)
        return self.specify_db_connection.get_records(sql=sql, params=params)

    def commit(self):
        """Commit changes to the SQLite database"""
        self.connection.commit()

    def convert_sql(self, sql):
        """Convert MySQL-style placeholders (%s) to SQLite-style (?)
            and append COLLATE NOCASE to `=` and `LIKE` conditions.
        """
        sql = sql.replace("%s", "?")

        # Ensure COLLATE NOCASE is applied to `=` and `LIKE` conditions
        sql = re.sub(r"(\b(?:WHERE|AND|OR)\b\s+[a-zA-Z0-9_`.\"()]+\s*(=|LIKE)\s*\?)", r"\1 COLLATE NOCASE", sql,
                     flags=re.IGNORECASE)

        return sql

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
        sql = self.convert_sql(sql)
        self.logger.debug(f"running query - {sql}")
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            self.commit()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            sys.exit("terminating script")
        cursor.close()
