from os import path

# global variable for use in paths
sla = path.sep
def basic_config():
    """initializes config dictionary and config variables common to all collections in 
       a dictionary format"""
    config = {}
    config['SPECIFY_DATABASE_HOST'] = "db.institution.org"
    config['SPECIFY_DATABASE_PORT'] = 3306
    return config

def initialize_collection_config(collection):
    """initialize_collection_config: initializes config dictionary for given collection name
        args:
            collection: a string input of the collection name, refer to the left column
            in collection_definitions.py
    """
    config = basic_config()
    if collection == "Botany_PIC":
        config['COLLECTION_NAME'] = "Botany"
    else:
        config['COLLECTION_NAME'] = collection

    config['MAILING_LIST'] = ["email_address"]
    config['REPORT_PATH'] = f"html_reports{sla}{collection.lower()}_import_monitoring.html"

    if "Botany" in collection:
        config = initialize_bot_picturae(config, collection)
    elif collection == "IZ":
        config = initialize_iz(config)
    elif collection == "Ichthyology":
        config = initialize_ich(config)
    elif collection == "picbatch":
        config = initialize_picbatch(config)
    else:
        raise ValueError("incorrect collection name")

    return config

def initialize_bot_picturae(config, collection):
    """intitializes config variables into a dictionary format for botany and
       botany picturae
    """

    config['SPECIFY_DATABASE'] = "casbotany"
    config['USER'] = "redacted"
    config['PASSWORD'] = "redacted"
    config['AGENT_ID'] = "your_agent_id"
    config['IMAGE_SUFFIX'] = "(CAS|cas)[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"

    if collection == "Botany_PIC":
        config['IMAGE_SUFFIX'] = "[0-9]*([-_])*[0-9a-zA-Z]?.(JPG|jpg|jpeg|TIFF|tif)"
        config['PREFIX'] = "path/to/directory"
        config['COLLECTION_PREFIX'] = f"image/folder"
        config['DATA_FOLDER'] = f"data_csv/folder"
        config['CSV_SPEC'] = f"{sla}picturae_specimen("
        config['CSV_FOLD'] = f"{sla}picturae_folder("
        config['SUMMARY_TERMS'] = []
        config['SUMMARY_IMG'] = []
        config['EXIF_DICT'] = {}
    else:
        config['PREFIX'] = f"{sla}Volumes"
        config['COLLECTION_PREFIX'] = "images"
        config['BOTANY_SCAN_FOLDERS'] = [f"botany{sla}PLANT FAMILIES"]
        config['SUMMARY_TERMS'] = []
        config['SUMMARY_IMG'] = []
        config['EXIF_DICT'] = {}

    return config

def initialize_iz(config):
    """initializes config variables in a
       dictionary format for the IZ collection
    """
    config['SPECIFY_DATABASE'] = "database"
    config['USER'] = "redacted"
    config['PASSWORD'] = "redacted"
    config['AGENT_ID'] = "your_agent_id"
    config['IMAGE_SUFFIX'] = '[a-z\-\(\)0-9 ©_,.]*(.(jpg|jpeg|tiff|tif|png|PNG))$'
    config['MINIMUM_ID_DIGITS'] = 5
    config['MAXIMUM_ID_DIGITS'] = 10
    config['CASIZ_NUMBER'] = "([0-9]{2,})"
    config['CASIZ_PREFIX'] = f"cas(iz)?[_ {sla}-]?"
    config['CASIZ_MATCH'] = config['CASIZ_PREFIX'] + config['CASIZ_NUMBER']
    config['FILENAME_MATCH'] = config['CASIZ_MATCH'] + config['IMAGE_SUFFIX']
    config['FILENAME_CONJUNCTION_MATCH'] = config['CASIZ_MATCH'] + \
                                           f' (and|or) ({config["CASIZ_PREFIX"]})?({config["CASIZ_NUMBER"]})'

    config['IZ_DIRECTORY_REGEX'] = config['CASIZ_MATCH']

    config['IZ_CORE_SCAN_FOLDERS'] = [f'.']

    config['PREFIX'] = f"{sla}"
    config['SUMMARY_TERMS'] = []
    config['SUMMARY_IMG'] = []
    config['EXIF_DICT'] = {}

    return config

def initialize_ich(config):
    """initializes config variables in a dictionary format
       for the Ichthyology collection
    """
    config['SPECIFY_DATABASE'] = "casich"
    config['USER'] = "username"
    config['PASSWORD'] = "password"
    config['AGENT_ID'] = "your_agent_id"
    config['IMAGE_DIRECTORY_PREFIX'] = f"{sla}letter_drives{sla}n_drive"
    config['SCAN_DIR'] = f"ichthyology{sla}images{sla}"
    config['ICH_SCAN_FOLDERS'] = ["AutomaticSpecifyImport"]
    config['SUMMARY_TERMS'] = []
    config['SUMMARY_IMG'] = []
    config['EXIF_DICT'] = {}

    return config

def initialize_picbatch(config):
    """initializes config variables in a dictionary format for
       the picturae picbatch testing DB
    """
    config['SPECIFY_DATABASE_HOST'] = "db.institution.org"
    config['SPECIFY_DATABASE_PORT'] = 3309
    config['SPECIFY_DATABASE'] = "picbatch"
    config['USER'] = "redacted"
    config['PASSWORD'] = "redacted"
    config['AGENT_ID'] = "your_agent_id"

    return config
