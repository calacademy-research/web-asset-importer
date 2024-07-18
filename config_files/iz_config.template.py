from os import path
sla = path.sep
# database credentials
SPECIFY_DATABASE_HOST = '127.0.0.1'
SPECIFY_DATABASE_PORT = 3318
SPECIFY_DATABASE = 'casiz'
USER = 'root'
PASSWORD = '123pass'

REPORT_PATH = f"html_reports{sla}iz_import_monitoring.html"
COLLECTION_NAME = "IZ"
MINIMUM_ID_DIGITS = 5
MAXIMUM_ID_DIGITS = 12

AGENT_ID = 123456

# path variables
MINIMUM_ID_DIGITS = 5
MAXIMUM_ID_DIGITS = 10
SHORT_MINIMUM_ID_DIGITS = 3
IMAGE_EXTENSION = r'(\.(jpg|jpeg|tiff|tif|png|dng))$'
IMAGE_SUFFIX = rf'[a-z\-\(\)0-9 ©_,.]*{IMAGE_EXTENSION}'
CASIZ_NUMBER = '([0-9]{' + str(MINIMUM_ID_DIGITS) + ','+ str(MAXIMUM_ID_DIGITS)+'})'
CASIZ_NUMBER_SHORT = '([0-9]{' + str(SHORT_MINIMUM_ID_DIGITS) + ','+ str(MAXIMUM_ID_DIGITS)+'})'
CASIZ_PREFIX = r'cas(iz)?[#a-z _]*[_ \-]?'
CASIZ_MATCH = rf'({CASIZ_PREFIX}{CASIZ_NUMBER_SHORT})|({CASIZ_NUMBER})'
FILENAME_MATCH = rf'{CASIZ_MATCH}{IMAGE_SUFFIX}'
FILENAME_CONJUNCTION_MATCH = rf'(({CASIZ_MATCH})|([ ]*(and|or)[ ]*({CASIZ_MATCH})))+'
DIRECTORY_CONJUNCTION_MATCH = FILENAME_CONJUNCTION_MATCH
DIRECTORY_MATCH = rf'{CASIZ_MATCH}'


IZ_SCAN_FOLDERS = [
    f'/volumes/data/izg/iz images',  # core images - hydra
    f'/Volumes/images/izg/iz',  # core images - pegasus
    f'/Volumes/data/izg/IZ Images/CASIZ Label Images' # label data
]
# IZ_SCAN_FOLDERS = [
#     f'/Users/joe/web-asset-importer/test_images'
# ]



REPORT_PATH = f"html_reports{sla}iz_import_monitoring.html"
# summary statistics, figures to configure html report
MAILING_LIST = []

SUMMARY_TERMS = []
SUMMARY_IMG = []

