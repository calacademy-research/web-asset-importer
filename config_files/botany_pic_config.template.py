from os import path
sla = path.sep

# database credentials
SPECIFY_DATABASE_HOST = "db.institution.org"
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = "database"
USER = "redacted"
PASSWORD = "redacted"

COLLECTION_NAME = "Botany"


REPORT_PATH = f"html_reports{sla}botany_pic_passive_import_monitoring.html"

ACTIVE_REPORT_PATH = f"html_reports{sla}botany_pic_active_import_monitoring.html"

# IMPORTER_AGENT_ID is used for allowing separate agent IDS for
# passive and active databased records, for easy purging.
IMPORTER_AGENT_ID = "designated importer agent_id"

AGENT_ID = "your_agent_id"

IMAGE_SUFFIX = "[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"

PREFIX = f"{sla}path{sla}to{sla}image{sla}folder"

PIC_SCAN_FOLDERS = f"CP1_MMDDYYYY_BATCH_0001{sla}"

FOLDER_REGEX = r"_.+?_"

PROJECT_NAME = "name of digitization project"


# summary statistics, figures to configure html report
MAILING_LIST = ['email_address']

AGENT_FIRST_TITLES = []
AGENT_LAST_TITLES = []

SUMMARY_TERMS = []
SUMMARY_IMG = []
EXIF_DICT = {}