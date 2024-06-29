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
        sql = f"""
        select cat.CollectionObjectID
               from attachment as at
               , collectionobjectattachment as cat

               where at.AttachmentLocation='{attachment_location}'
        and cat.AttachmentId = at.AttachmentId
        """
        coid = self.db_utils.get_one_record(sql)
        logging.debug(f"Got collectionObjectId: {coid}")

        return coid

    def get_attachmentid_from_filepath(self, orig_filepath):
        orig_filepath = repr(orig_filepath)
        sql = f"""
        select at.AttachmentID
               from attachment as at
               where at.OrigFilename={orig_filepath}
        """
        aid = self.db_utils.get_one_record(sql)
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
                          original_filename, url, file_created_datetime, guid, image_type,
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
            url,
            self.val(properties.get(SpecifyConstants.ST_SUBJECT_ORIENTATION), 'ST_SUBJECT_ORIENTATION'),
            self.val(properties.get(SpecifyConstants.ST_SUBTYPE), 'ST_SUBTYPE'),
            title_value,
            self.val(properties.get(SpecifyConstants.ST_TYPE), 'ST_TYPE'),
            agent_id
        )

        #         params = (
        #             attachment_location,
        #             val(properties.get(SpecifyConstants.ST_COPYRIGHT_DATE)),
        #             val(properties.get(SpecifyConstants.ST_COPYRIGHT_HOLDER)),
        #             val(properties.get(SpecifyConstants.ST_CREDIT)),
        #             val(properties.get(SpecifyConstants.ST_DATE_IMAGED)),
        #             file_created_datetime.strftime("%Y-%m-%d"),
        #             guid,
        #             properties.get(SpecifyConstants.ST_IS_PUBLIC, True),
        #             val(properties.get(SpecifyConstants.ST_LICENSE)),
        #             val(properties.get(SpecifyConstants.ST_LICENSE_LOGO_URL)),
        #             val(properties.get(SpecifyConstants.ST_METADATA_TEXT)),
        #             image_type,
        #             original_filename,
        #             url,
        #             val(properties.get(SpecifyConstants.ST_SUBJECT_ORIENTATION)),
        #             val(properties.get(SpecifyConstants.ST_SUBTYPE)),
        #             title_value,
        #             val(properties.get(SpecifyConstants.ST_TYPE)),
        #             agent_id
        #         )

        cursor = self.db_utils.get_cursor()
        cursor.execute(sql, params)
        self.db_utils.commit()
        cursor.close()

    def create_collection_object_attachment(self, attachment_id, collection_object_id, ordinal, agent_id):
        # 68835 Joe russack ich
        # 95728 Joe russack botany
        cursor = self.db_utils.get_cursor()

        sql = (f"""INSERT INTO collectionobjectattachment 
            (collectionmemberid, 
            ordinal, 
            remarks, 
            timestampcreated,
            timestampmodified, 
            version, 
            AttachmentID, 
            CollectionObjectID,
            CreatedByAgentID, 
            ModifiedByAgentID)
        VALUES (
            4, 
            {ordinal}, 
            NULL, 
            '{time_utils.get_pst_time_now_string()}', 
            '{time_utils.get_pst_time_now_string()}',
            0, 
            {attachment_id}, 
            {collection_object_id}, 
            {agent_id},
            NULL)""")
        cursor.execute(sql)
        self.db_utils.commit()
        cursor.close()

    def get_attachment_id(self, uuid):
        sql = f"select attachmentid from attachment where guid='{uuid}'"
        return self.db_utils.get_one_record(sql)

    def get_ordinal_for_collection_object_attachment(self, collection_object_id):
        sql = f"select max(ordinal) from collectionobjectattachment where CollectionObjectID={collection_object_id}"
        return self.db_utils.get_one_record(sql)

    def get_is_attachment_redacted(self, internal_id):
        sql = f"""
            select a.AttachmentID,a.ispublic  from attachment a
            where AttachmentLocation='{internal_id}'

            """
        cursor = self.db_utils.get_cursor()

        cursor.execute(sql)
        retval = cursor.fetchone()
        cursor.close()
        if retval is None:
            logging.error(f"Error fetching attachment internal id: {internal_id}\n sql:{sql}")
            raise db_utils.DatabaseInconsistentError()

        retval = retval[1]
        if retval is None:
            logging.warning(f"Warning: No results from: \n\n{sql}\n")
        else:
            if retval is False or retval == 0:
                return True
        return False

    def get_is_collection_object_redacted(self, collection_object_id):
        sql = f"""SELECT co.YesNo2          AS `CO redact locality`
             , vt.RedactLocality  AS `taxon_redact_locality`
             , vta.RedactLocality AS `accepted_taxon_redact_locality`
        FROM casbotany.collectionobject co
                 LEFT JOIN casbotany.determination de ON co.CollectionObjectID = de.CollectionObjectID AND de.IsCurrent = TRUE
                 LEFT JOIN casbotany.vtaxon2 vt ON de.TaxonID = vt.TaxonID
                 LEFT JOIN casbotany.vtaxon2 vta ON de.PreferredTaxonID = vta.taxonid
        WHERE co.CollectionObjectID = {collection_object_id};"""
        # logging.debug(f"isredacted sql: {sql}")
        cursor = self.db_utils.get_cursor()

        cursor.execute(sql)
        retval = cursor.fetchone()
        cursor.close()
        if retval is None:
            logging.error(f"Error fetching collection object id: {collection_object_id}\n sql:{sql}")
            raise DatabaseInconsistentError(f"DB error. SQL: {sql}")

        # logging.debug(f"Taxonid {retval[-1]}")
        # retval = retval[:4]
        if retval is None:
            logging.warning(f"Warning: No results from: \n\n{sql}\n")
        else:
            for val in retval:
                if val is True or val == 1 or val == b'\x01':
                    return True
        return False

    def get_is_taxon_id_redacted(self, taxon_id):
        """retrieves redacted boolean with taxon id from vtaxon2"""
        sql = f"""SELECT RedactLocality FROM vtaxon2 WHERE taxonid = {taxon_id};"""
        cursor = self.db_utils.get_cursor()
        cursor.execute(sql)
        retval = cursor.fetchone()
        cursor.close()
        if retval is None:
            logging.error(f"Error fetching taxon id: {taxon_id}\n sql:{sql}")
            return False
        else:
            for val in retval:
                if val is True or val == 1 or val == b'\x01':
                    return True
        return False


