SPECIFY_DATABASE_HOST = 'db.institution.org'
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = 'redacted'
USER = 'redacted'
PASSWORD = 'redacted'
# agent id in database
AGENT_ID = 123456
import os
sla = os.path.sep
COLLECTION_NAME = 'Botany'

BOTANY_REGEX = '(CAS|cas)[0-9]*([\-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)'
PREFIX = f"{sla}"
BOTANY_PREFIX = f'images'
BOTANY_SCAN_FOLDERS = [f'botany{sla}TYPE IMAGES',
                       f'botany{sla}PLANT FAMILIES']


# list of custom summary terms to add to monitoring email template
SUMMARY_TERMS = ['additional terms to summarize']

mailing_list = ['mdelaroca@calacademy.org']

