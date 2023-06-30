"""This file contains unit tests for picturae_import.py"""
import csv
import unittest
import random
import shutil
from picturae_import import *
from faker import Faker
from datetime import date, timedelta

# need to find a way to prevent the fake folders using todays date in the setUP,
# from overwriting the contents of real folders


def test_date():
    """test_date: creates an arbitrary date, 20 years in the past from today's date,
       to create test files for, so as not to overwrite current work
       ! if this code outlives 20 years of use I would be impressed"""
    unit_date = date.today() - timedelta(days=365 * 20)
    return str(unit_date)


class WorkingDirectoryTests(unittest.TestCase):
    """WorkingDirectoryTests: a series of unit tests to verify
        correct working directory, subdirectories."""

    def test_working_directory(self):
        """test if user in correct working folder picturae import"""
        expected_relative = "picturae_import"
        current_dir = os.getcwd()
        _, last_directory = os.path.split(current_dir)
        self.assertEqual(expected_relative, last_directory)

    def test_directory(self):
        """tests if working folder contains correct subdirectory picturae_csv"""
        dir_pre = os.path.isdir("picturae_csv")
        self.assertTrue(dir_pre)

    def test_missing_folder_raise_error(self):
        """checks if incorrect sub_directory raises error from file present"""
        date_string = test_date()
        with self.assertRaises(ValueError) as cm:
            file_present(date_string)
        self.assertEqual(str(cm.exception), f"subdirectory for {date.today()} not present")


class FilePathTests(unittest.TestCase):
    """ FilePathTests: tests paths for file_present
       function using dummy paths. """

    def setUp(self):
        """setUP: unittest setup function creates empty csvs,
                  folders for given test path"""

        print("setup called!")
        # create test directories

        date_string = test_date()

        expected_folder_path = 'picturae_csv/' + str(date_string) + '/picturae_folder(' + \
                               str(date_string) + ').csv'
        expected_specimen_path = 'picturae_csv/' + str(date_string) + '/picturae_specimen(' + \
                                 str(date_string) + ').csv'
        # making the directories
        os.makedirs(os.path.dirname(expected_folder_path), exist_ok=True)
        os.makedirs(os.path.dirname(expected_specimen_path), exist_ok=True)

        open(expected_folder_path, 'a').close()
        open(expected_specimen_path, 'a').close()

    def test_expected_path_date(self):
        """test_expected_path_date: makes temporary folders,
           and csvs with test_date, to test function"""
        date_string = test_date()
        try:
            file_present(date_string)
        except Exception as e:
            self.fail(f"Exception raised: {str(e)}")

    def test_raise_specimen(self):
        """test_raise_specimen: tests whether correct value
           error is raised for missing specimen csv"""
        date_string = test_date()
        # removing test path specimen
        os.remove('picturae_csv/' + str(date_string) + '/picturae_specimen(' +
                  str(date_string) + ').csv')
        with self.assertRaises(ValueError) as cm:
            file_present(date_string)
        self.assertEqual(str(cm.exception), "Specimen csv does not exist")

    def test_raise_folder(self):
        """test_raise_folder: tests whether correct value error
           is raised for missing folder csv"""
        date_string = test_date()
        # removing test path folder
        os.remove('picturae_csv/' + str(date_string) + '/picturae_folder(' +
                  str(date_string) + ').csv')
        with self.assertRaises(ValueError) as cm:
            file_present(date_string)
        self.assertEqual(str(cm.exception), "Folder csv does not exist")

    def tearDown(self):
        """destroys paths for Setup function,
           returning working directory to prior state"""

        print("teardown called!")

        date_string = test_date()
        # create test directories

        expected_folder_path = 'picturae_csv/' + str(date_string) + '/picturae_folder(' + \
                               str(date_string) + ').csv'
        shutil.rmtree(os.path.dirname(expected_folder_path))


# class for testing csv_import function
# under construction
class CsvReadMergeTests(unittest.TestCase):
    """this class contains methods which test outputs of the
       csv_read_folder function , and csv_merge functions from
       picturae_import.py"""

    # will think of ways to shorten this setup function
    def setUp(self):
        """creates fake datasets with dummy columns,
          that have a small subset of representive real column names,
          so that test merges and uploads can be performed.
          """
        print("setup called!")
        # setting num records and test date
        fake = Faker()
        num_records = 50
        date_string = test_date()
        # maybe create a separate function for setting up test directories
        path_type_list = ['folder', 'specimen']
        path_list = []
        for path_type in path_type_list:
            path = 'picturae_csv/' + str(date_string) + '/picturae_' + str(path_type) + '(' + \
                    str(date_string) + ').csv'

            path_list.append(path)

            os.makedirs(os.path.dirname(path), exist_ok=True)

            open(path, 'a').close()
        # writing csvs
        for path in path_list:
            with open(path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)

                writer.writerow(['specimen_barcode', 'folder_barcode', 'path_jpg'])
                for i in range(num_records):
                    # to keep barcodes matching between folder and specimen csvs for merging
                    ordered_bar = 123456
                    specimen_bar = ordered_bar + i
                    # populating rest of columns with random data
                    folder_barcode = fake.random_number(digits=6)
                    jpg_path = fake.file_path(depth=random.randint(1, 5), category='image', extension='jpg')

                    # writing data to CSV
                    writer.writerow([specimen_bar, folder_barcode, jpg_path])
            print(f"Fake dataset {path} with {num_records} records created sucessfuly")

    def test_file_empty(self):
        """tests for every argument variation if dataset returns as empty"""
        date_string = test_date()
        self.assertEqual(csv_read_folder("folder", import_date=date_string).empty, False)
        self.assertEqual(csv_read_folder("specimen", import_date=date_string).empty, False)

    def test_file_colnumber(self):
        """tests for every argument variation, if correct # of columns"""
        date_string = test_date()
        self.assertEqual(len(csv_read_folder('folder', import_date=date_string).columns), 3)
        self.assertEqual(len(csv_read_folder('specimen', import_date=date_string).columns), 3)

    def test_barcode_column_present(self):
        """tests for every argument variation, if barcode column is present
           (test if column names loaded correctly, specimen_barcode being in any csv)"""
        date_string = test_date()
        self.assertEqual('specimen_barcode' in csv_read_folder('folder', date_string).columns, True)
        self.assertEqual('specimen_barcode' in csv_read_folder('specimen', date_string).columns, True)

    # these tests are for the csv merge function
    def test_merge_num_columns(self):
        """test merge with sample data set , to check if shared columns are removed,
           and that the merge occurs with expected columns"""
        date_string = test_date()
        csv_specimen = csv_read_folder('specimen', import_date=date_string)
        csv_folder = csv_read_folder('folder', import_date=date_string)
        # -3 as merge function drops duplicate columns automatically
        self.assertEqual(len(csv_merge(csv_specimen, csv_folder).columns),
                         len(csv_specimen.columns) + len(csv_folder.columns) - 3)

    def test_index_length_matches(self):
        """checks whether dataframe, length changes,
           as folder and specimen csvs should
           always be 100% matches on barcodes
           """
        date_string = test_date()
        csv_folder = csv_read_folder('folder', import_date=date_string)
        csv_specimen = csv_read_folder('specimen', import_date=date_string)
        # test merge index before and after
        self.assertEqual(len(csv_merge(csv_folder, csv_specimen)),
                         len(csv_folder))

    def test_unequalbarcode_raise(self):
        """checks whether inserted errors in barcode raise
           a Value error raise in the merge function"""
        date_string = test_date()
        csv_folder = csv_read_folder('folder', date_string)
        csv_specimen = csv_read_folder('specimen', date_string)
        csv_specimen['specimen_barcode'] = csv_specimen['specimen_barcode'] + 1
        with self.assertRaises(ValueError) as cm:
            csv_merge(csv_folder, csv_specimen)

        self.assertEqual(str(cm.exception), "Barcode Columns do not match!")

    def test_output_isnot_empty(self):
        """tests whether merge function
           produces empty dataframe"""
        date_string = test_date()
        csv_folder = csv_read_folder('folder', import_date=date_string)
        csv_specimen = csv_read_folder('specimen', import_date=date_string)
        # testing output
        self.assertFalse(csv_merge(csv_folder, csv_specimen).empty)

    def tearDown(self):
        """deletes destination directories of dummy datasets"""
        print("teardown called!")
        date_string = test_date()

        folder_path = 'picturae_csv/' + str(date_string) + '/picturae_folder(' + \
                      str(date_string) + ').csv'

        print(os.path.dirname(folder_path))

        shutil.rmtree(os.path.dirname(folder_path))


if __name__ == "__main__":
    unittest.main()
