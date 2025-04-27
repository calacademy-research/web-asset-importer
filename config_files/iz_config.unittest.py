from os import path
import regex
sla = path.sep
# database credentials
SPECIFY_DATABASE_HOST = '127.0.0.1'
SPECIFY_DATABASE_PORT = 3306
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
CASIZ_NUMBER_EXACT = rf'{CASIZ_PREFIX}([0-9]+)'
FILENAME_MATCH = rf'{CASIZ_MATCH}{IMAGE_SUFFIX}'
FILENAME_CONJUNCTION_MATCH = rf'(({CASIZ_MATCH})|([ ]*(and|or)[ ]*({CASIZ_MATCH})))+'
DIRECTORY_CONJUNCTION_MATCH = FILENAME_CONJUNCTION_MATCH
DIRECTORY_MATCH = rf'{CASIZ_MATCH}'

REPORT_PATH = f"html_reports{sla}iz_import_monitoring.html"
# summary statistics, figures to configure html report
MAILING_LIST = []

SUMMARY_TERMS = []
SUMMARY_IMG = []

CASIZ_NUMBER_REGEX = regex.compile(
    r'''
    (?ix)                           # Ignore case, allow comments
    (?<!\w)                         # No word character before
    (                               
      (?:
        (?!IZACC[\s_-]?)             # Not IZACC prefix (negative lookahead)
        (?P<prefix>CASIZ|CAS)        # CASIZ or CAS (named group 'prefix')
        (?:[\s_-]*)                  # Spaces, underscores, dashes (zero or more)
      )?
      (?P<number>                    # --- Capture only the number ---
        (?!
          (?:DSC|P)\d{3,}            # Not digital camera serials
        )
        (?!
          (?<!CASIZ[\s_-]*|CAS[\s_-]*) # unless CASIZ/CAS prefix
          (?:19|20)\d{6}             # Dates like 20230412
        )
        \d{3,12}                     # 3-12 digits
      )
    )
    (?(prefix)                       # If prefix matched:
        (?=\D|$)                     # allow anything non-digit or end
    |
        (?=\b|[_\s-]|$)              # else must have boundary
    )
    ''', regex.VERBOSE
)

CASIZ_FALLBACK_REGEX = regex.compile(r'(?i)(?:CASIZ|CAS)[\s_-]*(\d{3,12})(?!\d)')