SPECIFY_DATABASE_HOST = 'db.institution.org'
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = 'redacted'
USER = 'redacted'
PASSWORD = 'redacted'
# agent id in database

# only needed if doing csv import
# AGENT_ID = 123456

import os
sla = os.path.sep
COLLECTION_NAME = 'Botany'

BOTANY_REGEX = '(CAS|cas)[0-9]*([\-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)'
PREFIX = f"{sla}"
BOTANY_PREFIX = f'images'
BOTANY_SCAN_FOLDERS = [f'botany{sla}TYPE IMAGES',
                       f'botany{sla}PLANT FAMILIES']


# config fields for monitoring emails
SUMMARY_TERMS = ['list of summary stats to add ']

SUMMARY_IMG = ['list of graph/image filepaths to add to report']

mailing_list = ['list of emails to send report to']

# testing smtp settings
# smtp_port = 587
# smtp_server = "smtp.gmail.com"
# smtp_user = "youremail@gmail.com"
# smtp_password = "generated app password"