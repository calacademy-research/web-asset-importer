
from os import path
sla = path.sep

## sections/collections

config = {}

section_list = ['Botany', 'Botany_PIC', 'Ichthyology', 'IZ', 'picbatch']

## initializing common DB port/hosts for all sections

for section in section_list:
    config[section] = {}
    config[section]['SPECIFY_DATABASE_HOST'] = "db.institution.org"
    config[section]['SPECIFY_DATABASE_PORT'] = 3306
    # html report path
    config[section]['REPORT_PATH'] = f"html_reports{sla}{section.lower()}_import_monitoring.html"
    # collection name var
    config[section]['COLLECTION_NAME'] = section

## Botany DB
for section in list(['Botany', 'Botany_PIC']):
    config[section]['SPECIFY_DATABASE'] = "casbotany"
    config[section]['USER'] = "redacted"
    config[section]['PASSWORD'] = "redacted"

## Botany

config['Botany']['AGENT_ID'] = 'your_agent_id'
config['Botany']['MAILING_LIST'] = ['email_address']

config['Botany']['IMAGE_SUFFIX'] = "(CAS|cas)[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"
config['Botany']['PREFIX'] = f"{sla}Volumes"
config['Botany']['COLLECTION_PREFIX'] = "images"
config['Botany']['BOTANY_SCAN_FOLDERS'] = [f"botany{sla}PLANT FAMILIES"]

config['Botany']['MAILING_LIST'] = ['email_address']

# summary statistics, figures to configure html report
config['Botany']['SUMMARY_TERMS'] = []
config['Botany']['SUMMARY_IMG'] = []

config['Botany']['EXIF_DICT'] = {}

## Botany_PIC

config['Botany']['AGENT_ID'] = 'your_agent_id'

config['Botany_PIC']['IMAGE_SUFFIX'] = "[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"

config['Botany_PIC']['PREFIX'] = f"pathto/directory"
config['Botany_PIC']['COLLECTION_PREFIX'] = f"path/to/images"
config['Botany_PIC']['DATA_FOLDER'] = f"path/to/csv_data"
config['Botany_PIC']['CSV_SPEC'] = f"{sla}picturae_specimen("
config['Botany_PIC']['CSV_FOLD'] = f"{sla}picturae_folder("

# summary statistics, figures to configure html report
config['Botany_PIC']['MAILING_LIST'] = ['email_address']

config['Botany_PIC']['SUMMARY_TERMS'] = []
config['Botany_PIC']['SUMMARY_IMG'] = []


config['Botany_PIC']['EXIF_DICT'] = {}

## Ichthyology

config['Ichthyology']['SPECIFY_DATABASE'] = "casich"
config['Ichthyology']['USER'] = "username"
config['Ichthyology']['PASSWORD'] = "password"
config['Ichthyology']['AGENT_ID'] = "your_agent_id"

config['Ichthyology']['IMAGE_DIRECTORY_PREFIX'] = f"{sla}letter_drives{sla}n_drive"
config['Ichthyology']['SCAN_DIR'] = f"ichthyology{sla}images{sla}"
config['Ichthyology']['ICH_SCAN_FOLDERS'] = ["AutomaticSpecifyImport"]

# summary statistics, figures to configure html report
config['Ichthyology']['MAILING_LIST'] = ['email_address']
config['Ichthyology']['SUMMARY_TERMS'] = []
config['Ichthyology']['SUMMARY_IMG'] = []

config['Ichthyology']['EXIF_DICT'] = {}

## IZ

config['IZ']['SPECIFY_DATABASE'] = "database"
config['IZ']['USER'] = "redacted"
config['IZ']['PASSWORD'] = "redacted"
config['IZ']['AGENT_ID'] = "your_agent_id"

config['IZ']['IMAGE_SUFFIX'] = "[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"
config['IZ']['MINIMUM_ID_DIGITS'] = 5
config['IZ']['MAXIMUM_ID_DIGITS'] = 10
config['IZ']['CASIZ_NUMBER'] = "([0-9]{2,})"
config['IZ']['CASIZ_PREFIX'] = f"cas(iz)?[_ {sla}-]?"

config['IZ']['CASIZ_MATCH'] = config['IZ']['CASIZ_PREFIX'] + config['IZ']['CASIZ_NUMBER']
config['IZ']['FILENAME_MATCH'] = config['IZ']['CASIZ_MATCH'] + config['IZ']['IMAGE_SUFFIX']
config['IZ']['FILENAME_CONJUNCTION_MATCH'] = config['IZ']['CASIZ_MATCH'] + \
                                       f' (and|or) ({config["IZ"]["CASIZ_PREFIX"]})?({config["IZ"]["CASIZ_NUMBER"]})'

config['IZ']['IZ_DIRECTORY_REGEX'] = config['IZ']['CASIZ_MATCH']

config['IZ']['IZ_CORE_SCAN_FOLDERS'] = [f'.']

config['IZ']['PREFIX'] = f"{sla}"
# summary statistics, figures to configure html report
config['IZ']['MAILING_LIST'] = ['email_address']

config['IZ']['SUMMARY_TERMS'] = []
config['IZ']['SUMMARY_IMG'] = []

config['IZ']['EXIF_DICT'] = {}

## picbatch

config['picbatch']['AGENT_ID'] = "your_agent_id"
config['picbatch']['SPECIFY_DATABASE_PORT'] = 3309
config['picbatch']['SPECIFY_DATABASE'] = "picbatch"
config['picbatch']['USER'] = "redacted"
config['picbatch']['PASSWORD'] = "redacted"


def get_config(section_name):
    """retrieves config section from 2D array defined above"""
    if section_name in section_list:
        section_config = config[f'{section_name}']
    else:
        raise KeyError(f'{section_name} not found')

    return section_config
