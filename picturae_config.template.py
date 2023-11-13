import os
COLLECTION_NAME = 'collectionname'
SPECIFY_DATABASE_HOST = 'db-institution.name'
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = 'redacted'
USER = 'redacted'
PASSWORD = 'redacted'

PIC_REGEX = '(CAS|cas)[0-9]*([\-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)'

sla = os.path.sep
# config paths files
date_str = None

PIC_PREFIX = f'picturae_img{sla}'
PIC_SCAN_FOLDERS = [f'PIC_{date_str}']
PREFIX = f"web-asset-server{sla}image_client{sla}"

# number for created by agent field , check your agent id on the agent table
AGENT_ID = 123456


DATA_FOLDER = f"picturae_csv{sla}"

CSV_SPEC = f"{sla}picturae_specimen("

CSV_FOLD = f"{sla}picturae_folder("

# list of custom summary terms to add to monitoring email template
SUMMARY_TERMS = ['Number of Taxa Added', "Number of Taxa Dropped by TNRS"]

# these are used for people who need batch monitoring
mailing_list = ['list of email addresses']


# testing smtp settings
# smtp_port = 587
# smtp_server = "smtp.gmail.com"
# smtp_user = "youremail@gmail.com"
# smtp_password = "generated app password"