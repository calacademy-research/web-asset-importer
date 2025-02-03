import mysql.connector
from mysql.connector import errorcode
import settings
from datetime import datetime
import logging
import time
from db_utils import DbUtils
TIME_FORMAT_NO_OFFSET = "%Y-%m-%d %H:%M:%S"
TIME_FORMAT = TIME_FORMAT_NO_OFFSET + "%z"

class ImageDb(DbUtils):
    def __init__(self):
        self.logger = logging.getLogger(f'Client.{self.__class__.__name__}')
        self.image_db_connection = super().__init__(
            settings.SQL_USER,
            settings.SQL_PASSWORD,
            settings.SQL_PORT,
            settings.SQL_HOST,
            settings.SQL_DATABASE)

    @staticmethod
    def retry_with_backoff(max_duration=10800, initial_delay=0):
        """Custom backoff logic with a shorter retry window."""
        return DbUtils.retry_with_backoff(max_duration, initial_delay)

    def create_tables(self):
        TABLES = {}

        TABLES['images'] = (
            "CREATE TABLE if not exists `images`.`images` ("
            "   id int NOT NULL AUTO_INCREMENT primary key,"
            "  `original_filename` varchar(2000),"
            "  `url` varchar(500),"
            "  `universal_url` varchar(500),"
            "  `original_path` varchar(2000),"
            "  `redacted` BOOLEAN,"
            "  `internal_filename` varchar(500),"
            "  `notes` varchar(8192),"
            "  `datetime` datetime,"
            "  `collection` varchar(50)"
            ") ENGINE=InnoDB")

        cursor = self.image_db_connection.get_cursor()

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                self.logger.info(f"Creating table {table_name}...")
                self.logger.info(f"Sql: {TABLES[table_name]}")
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    self.logger.error("already exists.")
                else:
                    self.logger.error(err.msg)
            else:
                self.logger.info("OK")

        cursor.close()
        cursor = self.image_db_connection.get_cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'images' AND column_name = 'orig_md5'")
        column_exists = cursor.fetchone()[0]

        if not column_exists:
            # Add the "orig_md5" column to the "images" table
            cursor.execute("ALTER TABLE images ADD COLUMN orig_md5 CHAR(32)")

        cursor.close()

    def create_image_record(self,
                            original_filename,
                            url,
                            internal_filename,
                            collection,
                            original_path,
                            notes,
                            redacted,
                            datetime_record,
                            original_image_md5
                            ):

        if original_filename is None:
            original_filename = "NULL"
        if original_image_md5 is None:
            original_image_md5 = "NULL"

        add_image = (f"""INSERT INTO images
                        (original_filename, url, universal_url, internal_filename, collection,original_path,notes,redacted,datetime,orig_md5)
                        values (
                        "{original_filename}", 
                        "{url}", 
                        NULL, 
                        "{internal_filename}", 
                        "{collection}", 
                        "{original_path}", 
                        "{notes}", 
                        "{int(redacted)}", 
                        "{datetime_record.strftime(TIME_FORMAT_NO_OFFSET)}",
                        "{original_image_md5}")""")
        self.image_db_connection.execute(add_image)
        self.logger.info(f"Inserting imageInserting image record. SQL: {add_image}")

    # is this necessary when we have self.execute or is this just debugging?
    @retry_with_backoff()
    def update_redacted(self, internal_filename, is_redacted):
        sql = f"""
        update images set redacted = {is_redacted} where internal_filename = '{internal_filename}' 
        """

        logging.debug(f"updating stop 0: {sql}")
        cursor = self.image_db_connection.get_cursor()
        logging.debug(f"updating stop 1")

        cursor.execute(sql)
        logging.debug(f"updating stop 2")

        self.image_db_connection.cnx.commit()
        logging.debug(f"updating stop 3")

        cursor.close()

    @retry_with_backoff()
    def get_record(self, where_clause):

        cursor = self.image_db_connection.get_cursor()

        query = f"""SELECT id, original_filename, url, universal_url, internal_filename, collection,original_path, notes, redacted, datetime, orig_md5
           FROM images 
           {where_clause}"""

        cursor.execute(query)
        record_list = []
        for (id, original_filename, url, universal_url, internal_filename, collection, original_path, notes,
                redacted, datetime_record, orig_md5) in cursor:
            record_list.append({'id': id,
                                'original_filename': original_filename,
                                'url': url,
                                'universal_url': universal_url,
                                'internal_filename': internal_filename,
                                'collection': collection,
                                'original_path': original_path,
                                'notes': notes,
                                'redacted': redacted,
                                'datetime': datetime.strptime(datetime_record, TIME_FORMAT),
                                'orig_md5': orig_md5
                                })

        cursor.close()
        return record_list

    @retry_with_backoff()
    def get_image_record_by_internal_filename(self, internal_filename):
        cursor = self.image_db_connection.get_cursor()

        query = f"""SELECT id, original_filename, url, universal_url, internal_filename, collection,original_path, notes, redacted, datetime, orig_md5
           FROM images 
           WHERE internal_filename = '{internal_filename}'"""

        cursor.execute(query)
        record_list = []
        for (id,
             original_filename,
             url,
             universal_url,
             internal_filename,
             collection,
             original_path,
             notes,
             redacted,
             datetime_record,
             orig_md5) in cursor:
            record_list.append({'id': id,
                                'original_filename': original_filename,
                                'url': url,
                                'universal_url': universal_url,
                                'internal_filename': internal_filename,
                                'collection': collection,
                                'original_path': original_path,
                                'notes': notes,
                                'redacted': redacted,
                                'datetime': datetime_record.strftime(TIME_FORMAT),
                                'orig_md5': orig_md5
                                })
        cursor.close()
        return record_list

    @retry_with_backoff()
    def get_image_record_by_pattern(self, pattern, column, exact, collection):
        cursor = self.image_db_connection.get_cursor()
        if exact:
            query = f"""SELECT id, original_filename, url, universal_url, internal_filename, collection,original_path, notes, redacted, datetime, orig_md5
            FROM images 
            WHERE {column} = '{pattern}'"""
        else:
            query = f"""SELECT id, original_filename, url, universal_url, internal_filename, collection,original_path, notes, redacted, datetime, orig_md5
            FROM images 
            WHERE {column} LIKE '{pattern}'"""

        if collection is not None:
            query += f""" AND collection = '{collection}'"""

        self.logger.info(f"Query get_image_record_by_{column}: {query}")


        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:
            pass
        else:
            self.logger.warning("No rows were returned.")

        self.logger.info(f"Fetched rows: {rows}")

        record_list = []

        for row in rows:
            (id, original_filename, url, universal_url, internal_filename, collection, original_path, notes,
             redacted, datetime_record, orig_md5) = row
            record_list.append({'id': id,
                                'original_filename': original_filename,
                                'url': url,
                                'universal_url': universal_url,
                                'internal_filename': internal_filename,
                                'collection': collection,
                                'original_path': original_path,
                                'notes': notes,
                                'redacted': redacted,
                                'datetime': datetime_record,
                                'orig_md5': orig_md5
                                })
            self.logger.info(f"Found at least one record: {record_list[-1]}")

        cursor.close()
        return record_list

    def get_image_record_by_original_path(self, original_path, exact, collection):
        record_list = self.get_image_record_by_pattern(original_path, 'original_path', exact, collection)
        return record_list

    def get_image_record_by_original_filename(self, original_filename, exact, collection):
        record_list = self.get_image_record_by_pattern(original_filename, 'original_filename', exact, collection)
        return record_list

    def get_image_record_by_original_image_md5(self, md5, collection):
        record_list = self.get_image_record_by_pattern(md5, 'orig_md5', True, collection)
        return record_list

    @retry_with_backoff()
    def delete_image_record(self, internal_filename):
        cursor = self.image_db_connection.get_cursor()

        delete_image = (f"""delete from images where internal_filename='{internal_filename}' ;""")

        self.logger.info(f"deleting image record. SQL: {delete_image}")
        cursor.execute(delete_image)
        self.image_db_connection.cnx.commit()
        cursor.close()

    @retry_with_backoff()
    def get_collection_list(self):
        cursor = self.image_db_connection.get_cursor()

        query = f"""select collection from collection"""

        cursor.execute(query)
        collection_list = []
        for (collection) in cursor:
            collection_list.append(collection)
    #
    #  not used 4/10/23 - left for referenece for now
    #

    # def search(self, filename, match_exact_data):
    #     params = {
    #         'filename': filename,
    #         'exact': match_exact_data,
    #         'token': self.generate_token(self.get_timestamp(), filename)
    #     }
    #
    #     r = requests.get(self.build_url("getImageRecordByOrigFilename"), params=params)
    #     print(f"Search result: {r.status_code}")
    #     if (r.status_code == 404):
    #         print(f"No records found for {arg}")
    #         return False
    #     if r.status_code != 200:
    #         print(f"Unexpected search result: {r.status_code}; aborting.")
    #         return
    #     data = json.loads(r.text)
    #     print(
    #         f"collection, datetime, id, internal_filename, notes, original filename, original path, redacted, universal URL, URL")
    #     if len(data) == 0:
    #         print("No match.")
    #     else:
    #         for item in data:
    #             print(
    #                 f"{item['collection']},{item['datetime']},{item['internal_filename']},{item['notes']},{item['original_filename']},{item['original_path']},{item['redacted']},{item['universal_url']},{item['url']}")
