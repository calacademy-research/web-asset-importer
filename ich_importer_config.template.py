import os
sla = os.path.sep
SPECIFY_DATABASE_HOST = 'db.insitution.org'
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = 'casich'
USER = 'redacrted'
PASSWORD = 'redacted'
COLLECTION_NAME='Ichthyology'


# only needed if doing csv import
# AGENT_ID = 123456

# agent id number in db
sla = os.path.sep
# final directory will be prefix + scan dir  and then iterate over all ICH_SCAN_FOLDERS
IMAGE_DIRECTORY_PREFIX = "/letter_drives/n_drive"
SCAN_DIR = f'ichthyology{sla}images{sla}'
ICH_SCAN_FOLDERS = ['AutomaticSpecifyImport']

# config fields for monitoring emails
SUMMARY_TERMS = ['list of summary stats to add ']

SUMMARY_IMG = ['list of graph/image filepaths to add to report']

mailing_list = ['list of emails to send report to']

#smpt terms
smtp_server = "localhost"
smtp_port = "port"
