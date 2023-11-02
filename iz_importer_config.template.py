import os
sla = os.path.sep

SPECIFY_DATABASE_HOST = 'db.insitution.org'
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = 'casiz'
USER = 'redacrted'
PASSWORD = 'redacted'

COLLECTION_NAME = 'IZ'

# only needed if doing csv import
# AGENT_ID = 123456

MINIMUM_ID_DIGITS = 5
IMAGE_SUFFIX = '[a-z\-\(\)0-9 ©_,.]*(.(jpg|jpeg|tiff|tif|png|PNG))$'
CASIZ_NUMBER = '([0-9]{2,})'
CASIZ_PREFIX = 'cas(iz)?[_ \-]?'
CASIZ_MATCH = CASIZ_PREFIX + CASIZ_NUMBER
FILENAME_MATCH = CASIZ_MATCH + IMAGE_SUFFIX
FILENAME_CONJUNCTION_MATCH = CASIZ_MATCH + f' (and|or) ({CASIZ_PREFIX})?({CASIZ_NUMBER})'



IZ_DIRECTORY_REGEX = CASIZ_MATCH

PREFIX = f"{sla}"


IZ_CORE_SCAN_FOLDERS = [
    f'.'
]
# https://exiv2.org/tags.html
EXIF_DECODER_RING = {
    315: 'Artist',
    33432: 'Copyright',
    270: 'ImageDescription'
}



# config fields for monitoring emails
SUMMARY_TERMS = ['list of summary stats to add ']

SUMMARY_IMG = ['list of graph/image filepaths to add to report']

mailing_list = ['list of emails to send report to']

# testing smtp settings
# smtp_port = 587
# smtp_server = "smtp.gmail.com"
# smtp_user = "youremail@gmail.com"
# smtp_password = "generated app password"