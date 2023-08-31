import picturae_config as picturae_config
from importer import Importer
import traceback
import pandas as pd


class CsvDatabase(Importer):
    def __init__(self, number):
        super().__init__(picturae_config, "Botany")
        self.purge_code = number

    def sql_time_purger(self, database, table, timestamp1, timestamp2):
        try:
            cursor = self.specify_db_connection.get_cursor()
        except Exception as e:
            self.logger.error(f"Connection Error: {e}")

        sql = f'''DELETE FROM {database}.{table} WHERE TimestampCreated >= 
                  "{timestamp1}" AND TimestampCreated <= "{timestamp2}"'''
        self.logger.info(f'running query: {sql}')
        print(sql)
        self.logger.debug(sql)
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f"Exception thrown while processing sql: {sql}\n{e}\n", flush=True)
            self.logger.error(traceback.format_exc())

        self.specify_db_connection.commit()

        cursor.close()

    def sql_tree_purger(self, database, timestamp1, timestamp2):

        try:
            cursor = self.specify_db_connection.get_cursor()
        except Exception as e:
            self.logger.error(f"Connection Error: {e}")
        sql_temp = f'''CREATE TEMPORARY TABLE temp_leaf_nodes AS SELECT TaxonID FROM {database}.taxon WHERE TaxonID
                       NOT IN (SELECT DISTINCT ParentID FROM {database}.taxon
                       WHERE ParentID IS NOT NULL) 
                       AND TimestampCreated >= "{timestamp1}" AND TimestampCreated <= "{timestamp2}";'''

        sql_del = f'''DELETE FROM {database}.taxon WHERE TaxonID IN (SELECT TaxonID FROM temp_leaf_nodes);'''

        sql_drop = f'''DROP TEMPORARY TABLE IF EXISTS temp_leaf_nodes;'''
        rows_affected = 0
        while True:
            try:
                cursor.execute(sql_temp)
                print(sql_temp)
                cursor.execute(sql_del)
                print(sql_del)
                rows_affected = cursor.rowcount
                cursor.execute(sql_drop)
                print(sql_drop)
            except Exception as e:
                print(f"Exception thrown while processing sql: \n{e}\n", flush=True)
                self.logger.error(traceback.format_exc())

            if rows_affected == 0:
                break
        self.specify_db_connection.commit()

        cursor.close()

    def casbotany_csv_purger(self, number: int):
        """casbotany_csv_purger: runs sql commands to database, to purge sql records created between two timestamps
                                 in which the original upload script was run,
            uses log of sql uploads to retrieve record by randomly generated 6 digit upload code.
        """
        time_stamp_csv = pd.read_csv('test_csv_purge_sql/upload_time_stamps.csv')

        selected_row = time_stamp_csv[time_stamp_csv['UploadCode'] == number]

        time_stamp_list = []

        time_stamp_list.append(selected_row['StartTime'].to_string(index=False))
        time_stamp_list.append(selected_row['EndTime'].to_string(index=False))

        table_list = ['collectionobjectattachment', 'attachment',
                      'determination', 'collectionobject', 'collector',
                      'collectingevent', 'locality', 'agent']

        for table in table_list:
            self.sql_time_purger(database='casbotany', table=table,
                                 timestamp1=time_stamp_list[0],
                                 timestamp2=time_stamp_list[1])


        self.sql_tree_purger(database='casbotany',
                             timestamp1=time_stamp_list[0],
                             timestamp2=time_stamp_list[1])

    def run_all(self):
        self.casbotany_csv_purger(number=self.purge_code)



def master_run():
    purge_int = CsvDatabase(number=888220)
    purge_int.run_all()


master_run()

# casbotany_csv_purger(number=476987)


# class_int = CsvDatabase()
#
# class_int.sql_time_purger("casbotany", "collectionobjectattachment",
#                           "2023-07-28 09:28:13.238155", "2023-07-28 09:28:45.936700")