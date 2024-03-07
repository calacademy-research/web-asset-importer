from os import path
sla = path.sep
# database credentials
SPECIFY_DATABASE_HOST = "db.institution.org"
SPECIFY_DATABASE_PORT = 3306
SPECIFY_DATABASE = "database"
USER = "redacted"
PASSWORD = "redacted"

COLLECTION_NAME = "Botany"

REPORT_PATH = f"html_reports{sla}botany_import_monitoring.html"

AGENT_ID = "your_agent_id"

# path variables

IMAGE_SUFFIX = "(CAS|cas)[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"
PREFIX = f"{sla}Volumes"
COLLECTION_PREFIX = "images"
BOTANY_SCAN_FOLDERS = [f"botany{sla}PLANT FAMILIES"]

# summary statistics, figures to configure html report
MAILING_LIST = ["email_address"]

SUMMARY_TERMS = []
SUMMARY_IMG = []

EXIF_DICT = {}