from os import path
sla = path.sep
# database credentials
SPECIFY_DATABASE_HOST = "db.institution.org"
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = "database"
USER = "redacted"
PASSWORD = "redacted"

REPORT_PATH = f"html_reports{sla}iz_import_monitoring.html"
COLLECTION_NAME = "IZ"

AGENT_ID = 123456

# path variables
IMAGE_SUFFIX = "[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"
MINIMUM_ID_DIGITS = 5
MAXIMUM_ID_DIGITS = 10
CASIZ_NUMBER = "([0-9]{2,})"
CASIZ_PREFIX = f"cas(iz)?[_ {sla}-]?"

CASIZ_MATCH = CASIZ_PREFIX + CASIZ_NUMBER
FILENAME_MATCH = CASIZ_MATCH + IMAGE_SUFFIX
FILENAME_CONJUNCTION_MATCH = CASIZ_MATCH + f' (and|or) ({CASIZ_PREFIX})?({CASIZ_NUMBER})'

IZ_DIRECTORY_REGEX = CASIZ_MATCH

IZ_CORE_SCAN_FOLDERS = [f'.']

PREFIX = f"{sla}"

# summary statistics, figures to configure html report
MAILING_LIST = []

SUMMARY_TERMS = []
SUMMARY_IMG = []

EXIF_DICT = {}
