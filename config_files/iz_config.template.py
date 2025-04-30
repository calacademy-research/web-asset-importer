from os import path
import regex
sla = path.sep
# database credentials
SPECIFY_DATABASE_HOST = '127.0.0.1'
SPECIFY_DATABASE_PORT = 3318
SPECIFY_DATABASE = 'casiz'
USER = 'root'
PASSWORD = '123pass'

REPORT_PATH = f"html_reports{sla}iz_import_monitoring.html"
COLLECTION_NAME = "IZ"

AGENT_ID = 123456

# path variables
IMAGE_EXTENSION = r'(\.(jpg|jpeg|tiff|tif|png|dng))$'
IMAGE_SUFFIX = rf'[a-z\-\(\)0-9 ©_,.]*{IMAGE_EXTENSION}'


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

MINIMUM_ID_DIGITS_WITH_PREFIX = 3
MAXIMUM_ID_DIGITS = 12
MINIMUM_ID_DIGITS = 5

CASIZ_NUMBER_REGEX = regex.compile(
    r'''
    (?ix)                           # Ignore case, allow comments
    (?<!\w)                         # No word character before
    (                               
      (?:
        (?!IZACC[\s_#-]?)             # Not IZACC prefix (negative lookahead)
        (?P<prefix>CASIZ|CAS)        # CASIZ or CAS (named group 'prefix')
        (?:[\s_#-]*)                  # Spaces, underscores, dashes (zero or more)
      )?
      (?P<number>                    # --- Capture only the number ---
        (?!
          (?:DSC|P)\d{{{min_digits_with_prefix},}}     # Not camera serials
        )
        (?!
          (?<!CASIZ[\s_#-]*|CAS[\s_#-]*)
          (?:(?:19|20)\d{{2}}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))
        )
        \d{{{min_digits_with_prefix},{max_digits}}}
      )
    )
    (?(prefix)
        (?=\D|$)
    |
        (?=\b|[_\s#-]|$)
    )
    '''.format(
        min_digits_with_prefix=MINIMUM_ID_DIGITS_WITH_PREFIX,
        max_digits=MAXIMUM_ID_DIGITS
    ),
    regex.VERBOSE
)


CASIZ_FALLBACK_REGEX = regex.compile(
    r'(?i)(?:CASIZ|CAS)[\s_#-]*(\d{{{min_digits},{max_digits}}})(?!\d)'.format(
        min_digits=MINIMUM_ID_DIGITS_WITH_PREFIX,
        max_digits=MAXIMUM_ID_DIGITS
    )
)