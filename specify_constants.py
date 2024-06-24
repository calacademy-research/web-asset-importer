from metadata_tools.base_constants import BaseConstants

class SpecifyConstants(BaseConstants):
    # Table Nomenclature Constants
    ST_ATTACHMENT_LOCATION = 'attachmentlocation'
    ST_ATTACHMENT_STORAGE_CONFIG = 'attachmentstorageconfig'
    ST_CAPTURE_DEVICE = 'capturedevice'
    ST_COPYRIGHT_DATE = 'copyrightdate'
    ST_COPYRIGHT_HOLDER = 'copyrightholder'
    ST_CREDIT = 'credit'
    ST_DATE_IMAGED = 'dateimaged'
    ST_FILE_CREATED_DATE = 'filecreateddate'
    ST_GUID = 'guid'
    ST_IS_PUBLIC = 'ispublic'
    ST_LICENSE = 'license'
    ST_LICENSE_LOGO_URL = 'licenselogourl'
    ST_METADATA_TEXT = 'metadatatext'
    ST_MIME_TYPE = 'mimetype'
    ST_ORIG_FILENAME = 'origfilename'
    ST_REMARKS = 'remarks'
    ST_SCOPE_ID = 'scopeid'
    ST_SCOPE_TYPE = 'scopetype'
    ST_SUBJECT_ORIENTATION = 'subjectorientation'
    ST_SUBTYPE = 'subtype'
    ST_TABLE_ID = 'tableid'
    ST_TIMESTAMP_CREATED = 'timestampcreated'
    ST_TIMESTAMP_MODIFIED = 'timestampmodified'
    ST_TITLE = 'title'
    ST_TYPE = 'type'
    ST_VERSION = 'version'
    ST_VISIBILITY = 'visibility'
    ST_ATTACHMENT_IMAGE_ATTRIBUTE_ID = 'AttachmentImageAttributeID'
    ST_CREATED_BY_AGENT_ID = 'CreatedByAgentID'
    ST_CREATOR_ID = 'CreatorID'
    ST_MODIFIED_BY_AGENT_ID = 'ModifiedByAgentID'
    ST_VISIBILITY_SET_BY_ID = 'VisibilitySetByID'

    # Maximum Length Constants
    MAXLEN_ST_ATTACHMENT_LOCATION = 128
    MAXLEN_ST_COPYRIGHT_DATE = 64
    MAXLEN_ST_COPYRIGHT_HOLDER = 64
    MAXLEN_ST_CREDIT = 64
    MAXLEN_ST_DATE_IMAGED = 64
    MAXLEN_ST_GUID = 128
    MAXLEN_ST_LICENSE = 64
    MAXLEN_ST_LICENSE_LOGO_URL = 256
    MAXLEN_ST_METADATA_TEXT = 256
    MAXLEN_ST_MIME_TYPE = 1024
    MAXLEN_ST_ORIG_FILENAME = None  # mediumtext, no specific max length in MySQL
    MAXLEN_ST_REMARKS = None  # text, no specific max length in MySQL
    MAXLEN_ST_SCOPE_ID = None  # integer, max length not applicable
    MAXLEN_ST_SCOPE_TYPE = None  # tinyint, max length not applicable
    MAXLEN_ST_TABLE_ID = None  # smallint, max length not applicable
    MAXLEN_ST_TITLE = 255
    MAXLEN_ST_VISIBILITY = None  # tinyint, max length not applicable
    MAXLEN_ST_VISIBILITY_SET_BY_ID = None  # integer, max length not applicable
    MAXLEN_ST_ATTACHMENT_IMAGE_ATTRIBUTE_ID = None  # integer, max length not applicable
    MAXLEN_ST_MODIFIED_BY_AGENT_ID = None  # integer, max length not applicable
    MAXLEN_ST_CREATED_BY_AGENT_ID = None  # integer, max length not applicable
    MAXLEN_ST_IS_PUBLIC = None  # bit, max length not applicable
    MAXLEN_ST_CREATOR_ID = None  # integer, max length not applicable
    MAXLEN_ST_CAPTURE_DEVICE = 128
    MAXLEN_ST_SUBJECT_ORIENTATION = 64
    MAXLEN_ST_SUBTYPE = 64
    MAXLEN_ST_TYPE = 64
    MAXLEN_ST_ATTACHMENT_STORAGE_CONFIG = None  # text, no specific max length in MySQL




