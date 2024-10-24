
from os import path
sla = path.sep

# database credentials
SPECIFY_DATABASE_HOST = "db.institution.org"
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = "database"
USER = "redacted"
PASSWORD = "redacted"

COLLECTION_NAME = "Ichthyology"

REPORT_PATH = f"html_reports{sla}ichthyology_import_monitoring.html"

AGENT_ID = 123456

# path variables
IMAGE_DIRECTORY_PREFIX = f"{sla}letter_drives{sla}n_drive"
SCAN_DIR = f"ichthyology{sla}images{sla}"
ICH_SCAN_FOLDERS = ["AutomaticSpecifyImport"]

# summary statistics, figures to configure html report
MAILING_LIST = []

SUMMARY_TERMS = []
SUMMARY_IMG = []
EXIF_DICT = {}
