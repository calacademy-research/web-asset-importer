import time_utils
import db_utils
from db_utils import DatabaseInconsistentError
import logging
from metadata_tools.EXIF_constants import EXIFConstants
import os
from specify_constants import SpecifyConstants


class AttachmentUtils:

    def __init__(self, db_utils):
        self.db_utils = db_utils

    def get_collectionobjectid_from_filename(self, attachment_location):
        sql = """
        SELECT cat.CollectionObjectID
        FROM attachment AS at
        JOIN collectionobjectattachment AS cat ON cat.AttachmentId = at.AttachmentId
        WHERE at.AttachmentLocation = %s
        """
        params = (str(attachment_location) if attachment_location is not None else None,)
        coid = self.db_utils.get_one_record(sql, params)
        logging.debug(f"Got collectionObjectId: {coid}")
        return coid

    def get_attachmentid_from_filepath(self, orig_filepath):
        sql = """
         SELECT at.AttachmentID
         FROM attachment AS at
         WHERE at.OrigFilename = %s
         """
        params = (str(orig_filepath) if orig_filepath is not None else None,)
        aid = self.db_utils.get_one_record(sql, params)
        if aid is not None:
            logging.debug(f"Got AttachmentId: {aid}")
        return aid

    @staticmethod
    def truncate(value, max_length, field_name):
        if value is not None and max_length is not None and len(value) > max_length:
            truncated_value = value[:max_length]
            logging.warning(f"Value '{value}' for field '{field_name}' exceeds max length of {max_length} and has been truncated to '{truncated_value}'")
            return truncated_value
        return value

    def val(self, value, field_name):
        if value in [None, 'NULL']:
            return None
        max_length_attr = f"MAXLEN_{field_name.upper()}"
        max_length = getattr(SpecifyConstants, max_length_attr, None)
        return self.truncate(value, max_length, field_name)

    def create_attachment(self, attachment_location,
                          original_filename, file_created_datetime, guid, image_type,
                          agent_id,
                          properties):


        # parsing title
        basename = os.path.basename(original_filename)
        title_value = f'{".".join(basename.split(".")[:-1])}'



        # Using parameterized SQL queries to prevent SQL injection
        sql = f"""
                INSERT INTO attachment (
                    {SpecifyConstants.ST_ATTACHMENT_LOCATION}, {SpecifyConstants.ST_ATTACHMENT_STORAGE_CONFIG}, {SpecifyConstants.ST_CAPTURE_DEVICE}, {SpecifyConstants.ST_COPYRIGHT_DATE}, {SpecifyConstants.ST_COPYRIGHT_HOLDER}, {SpecifyConstants.ST_CREDIT},
                    {SpecifyConstants.ST_DATE_IMAGED}, {SpecifyConstants.ST_FILE_CREATED_DATE}, {SpecifyConstants.ST_GUID}, {SpecifyConstants.ST_IS_PUBLIC}, {SpecifyConstants.ST_LICENSE}, {SpecifyConstants.ST_LICENSE_LOGO_URL}, {SpecifyConstants.ST_METADATA_TEXT}, {SpecifyConstants.ST_MIME_TYPE},
                    {SpecifyConstants.ST_ORIG_FILENAME}, {SpecifyConstants.ST_REMARKS}, {SpecifyConstants.ST_SCOPE_ID}, {SpecifyConstants.ST_SCOPE_TYPE}, {SpecifyConstants.ST_SUBJECT_ORIENTATION}, {SpecifyConstants.ST_SUBTYPE}, {SpecifyConstants.ST_TABLE_ID}, {SpecifyConstants.ST_TIMESTAMP_CREATED},
                    {SpecifyConstants.ST_TIMESTAMP_MODIFIED}, {SpecifyConstants.ST_TITLE}, {SpecifyConstants.ST_TYPE}, {SpecifyConstants.ST_VERSION}, {SpecifyConstants.ST_VISIBILITY}, {SpecifyConstants.ST_ATTACHMENT_IMAGE_ATTRIBUTE_ID}, {SpecifyConstants.ST_CREATED_BY_AGENT_ID},
                    {SpecifyConstants.ST_CREATOR_ID}, {SpecifyConstants.ST_MODIFIED_BY_AGENT_ID}, {SpecifyConstants.ST_VISIBILITY_SET_BY_ID}
                )
                VALUES (
                    %s, NULL, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 4, 0, %s, %s, 1, CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP, %s, %s, 0, NULL, NULL, %s, NULL, NULL, NULL
                )
            """

        # self val already checks for none so no need for if not none logic
        params = (
            attachment_location,
            self.val(properties.get(SpecifyConstants.ST_COPYRIGHT_DATE), 'ST_COPYRIGHT_DATE'),
            self.val(properties.get(SpecifyConstants.ST_COPYRIGHT_HOLDER), 'ST_COPYRIGHT_HOLDER'),
            self.val(properties.get(SpecifyConstants.ST_CREDIT), 'ST_CREDIT'),
            self.val(properties.get(SpecifyConstants.ST_DATE_IMAGED), 'ST_DATE_IMAGED'),
            file_created_datetime.strftime("%Y-%m-%d"),
            guid,
            properties.get(SpecifyConstants.ST_IS_PUBLIC, True),
            self.val(properties.get(SpecifyConstants.ST_LICENSE), 'ST_LICENSE'),
            self.val(properties.get(SpecifyConstants.ST_LICENSE_LOGO_URL), 'ST_LICENSE_LOGO_URL'),
            self.val(properties.get(SpecifyConstants.ST_METADATA_TEXT), 'ST_METADATA_TEXT'),
            image_type,
            original_filename,
            self.val(properties.get(SpecifyConstants.ST_REMARKS), 'ST_REMARKS'),
            self.val(properties.get(SpecifyConstants.ST_SUBJECT_ORIENTATION), 'ST_SUBJECT_ORIENTATION'),
            self.val(properties.get(SpecifyConstants.ST_SUBTYPE), 'ST_SUBTYPE'),
            title_value,
            self.val(properties.get(SpecifyConstants.ST_TYPE), 'ST_TYPE'),
            agent_id
        )

        cursor = self.db_utils.get_cursor()
        cursor.execute(sql, params)
        self.db_utils.commit()
        cursor.close()

    def create_collection_object_attachment(self, attachment_id, collection_object_id, ordinal, agent_id):
        sql = """
        INSERT INTO collectionobjectattachment (
            collectionmemberid, ordinal, remarks, timestampcreated, timestampmodified,
            version, AttachmentID, CollectionObjectID, CreatedByAgentID, ModifiedByAgentID
        ) VALUES (
            4, %s, NULL, %s, %s, 0, %s, %s, %s, NULL
        )
        """
        params = (
            ordinal if ordinal is not None else None,
            time_utils.get_pst_time_now_string() if ordinal is not None else None,
            time_utils.get_pst_time_now_string() if ordinal is not None else None,
            attachment_id if ordinal is not None else None,
            collection_object_id if ordinal is not None else None,
            agent_id if ordinal is not None else None
        )
        cursor = self.db_utils.get_cursor()
        cursor.execute(sql, params)
        self.db_utils.commit()
        cursor.close()


    def get_attachment_id(self, uuid):
        sql = "SELECT attachmentid FROM attachment WHERE guid = %s"
        return self.db_utils.get_one_record(sql, (uuid,))

    def get_ordinal_for_collection_object_attachment(self, collection_object_id):
        sql = "SELECT MAX(ordinal) FROM collectionobjectattachment WHERE CollectionObjectID = %s"
        return self.db_utils.get_one_record(sql, (collection_object_id,))

    def get_is_attachment_redacted(self, internal_id):
        sql = """
        SELECT ispublic FROM attachment WHERE AttachmentLocation = %s
        """
        cursor = self.db_utils.get_cursor()
        params = (str(internal_id) if internal_id is not None else None,)
        cursor.execute(sql, params)
        retval = cursor.fetchone()
        cursor.close()

        if retval is None:
            logging.error(f"Error fetching attachment internal id: {internal_id}\n SQL: {sql}")
            raise DatabaseInconsistentError()
        return retval[0] in [False, 0]

    def get_is_botany_collection_object_redacted(self, collection_object_id):
        sql = """
        SELECT co.YesNo2, vt.RedactLocality, vta.RedactLocality
        FROM casbotany.collectionobject co
        LEFT JOIN casbotany.determination de ON co.CollectionObjectID = de.CollectionObjectID AND de.IsCurrent = TRUE
        LEFT JOIN casbotany.vtaxon2 vt ON de.TaxonID = vt.TaxonID
        LEFT JOIN casbotany.vtaxon2 vta ON de.PreferredTaxonID = vta.taxonid
        WHERE co.CollectionObjectID = %s
        """
        cursor = self.db_utils.get_cursor()
        params = (str(collection_object_id) if collection_object_id is not None else None,)
        cursor.execute(sql, params)
        retval = cursor.fetchone()
        cursor.close()

        if retval is None:
            logging.error(f"Error fetching collection object id: {collection_object_id}\n SQL: {sql}")
            raise DatabaseInconsistentError()
        return any(val in [True, 1, b'\x01'] for val in retval)
