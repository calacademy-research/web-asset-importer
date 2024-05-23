import time_utils
import db_utils
from db_utils import DatabaseInconsistentError
import logging
import os

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

    # not used - can be multiple in IZ, so dropping
    # def get_collectionobjectid_from_filepath(self, attachment_location):
    #     sql = f"""
    #     select cat.CollectionObjectID
    #            from attachment as at
    #            , collectionobjectattachment as cat
    #
    #            where at.AttachmentLocation='{attachment_location}'
    #     and cat.AttachmentId = at.AttachmentId
    #     """
    #     coid = self.db_utils.get_one_record(sql)
    #     logging.debug(f"Got collectionObjectId: {coid}")
    #
    #     return coid

    def old_deleteme_create_attachment(self, storename, original_filename, file_created_datetime, guid, image_type, url, agent_id,
                          copyright=None, is_public=True):
        # image type example 'image/png'
        basename = os.path.basename(original_filename)
        sql = (f"""
                INSERT INTO attachment (attachmentlocation, attachmentstorageconfig, capturedevice, copyrightdate,
                                          copyrightholder, credit, dateimaged, filecreateddate, guid, ispublic, license,
                                          licenselogourl, metadatatext, mimetype, origfilename, remarks, scopeid,
                                          scopetype, subjectorientation, subtype, tableid, timestampcreated,
                                          timestampmodified, title, type, version, visibility, AttachmentImageAttributeID,
                                          CreatedByAgentID, CreatorID, ModifiedByAgentID, VisibilitySetByID)
                VALUES ('{storename}', NULL, NULL, NULL, 
                        '{copyright}', NULL, NULL, '{time_utils.get_pst_date_time_from_datetime(time_utils.get_pst_time(file_created_datetime))}', '{guid}', {is_public}, NULL, 
                        NULL, NULL, '{image_type}','{original_filename}', '{url}', 4, 
                        0, NULL, NULL, 41, '{time_utils.get_pst_time_now_string()}',
                        '{time_utils.get_pst_time_now_string()}', '{".".join(basename.split(".")[:-1])}', NULL, 0, NULL, NULL, 
                        {agent_id}, NULL, NULL, NULL)
        """)
        cursor = self.db_utils.get_cursor()
        cursor.execute(sql)
        self.db_utils.commit()
        cursor.close()

    # Constants for specify table nomenclature
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

    def create_attachment(self, storename, original_filename, file_created_datetime, guid, image_type, agent_id,
                          properties):
        # Helper function to handle None values correctly for SQL
        def val(param):
            return param if param is not None else 'NULL'

        # Using parameterized SQL queries to prevent SQL injection
        sql = f"""
            INSERT INTO attachment (
                {ST_ATTACHMENT_LOCATION}, {ST_ATTACHMENT_STORAGE_CONFIG}, {ST_CAPTURE_DEVICE}, {ST_COPYRIGHT_DATE}, {ST_COPYRIGHT_HOLDER}, {ST_CREDIT},
                {ST_DATE_IMAGED}, {ST_FILE_CREATED_DATE}, {ST_GUID}, {ST_IS_PUBLIC}, {ST_LICENSE}, {ST_LICENSE_LOGO_URL}, {ST_METADATA_TEXT}, {ST_MIME_TYPE},
                {ST_ORIG_FILENAME}, {ST_REMARKS}, {ST_SCOPE_ID}, {ST_SCOPE_TYPE}, {ST_SUBJECT_ORIENTATION}, {ST_SUBTYPE}, {ST_TABLE_ID}, {ST_TIMESTAMP_CREATED},
                {ST_TIMESTAMP_MODIFIED}, {ST_TITLE}, {ST_TYPE}, {ST_VERSION}, {ST_VISIBILITY}, {ST_ATTACHMENT_IMAGE_ATTRIBUTE_ID}, {ST_CREATED_BY_AGENT_ID},
                {ST_CREATOR_ID}, {ST_MODIFIED_BY_AGENT_ID}, {ST_VISIBILITY_SET_BY_ID}
            )
            VALUES (
                %s, NULL, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 41, CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP, %s, %s, 0, NULL, NULL, %s, NULL, NULL, NULL
            )
        """
        params = (
            storename,
            val(properties.get(ST_COPYRIGHT_DATE)),
            val(properties.get(ST_COPYRIGHT_HOLDER)),
            val(properties.get(ST_CREDIT)),
            val(properties.get(ST_DATE_IMAGED)),
            file_created_datetime.strftime("%Y-%m-%d"),
            guid,
            properties.get(ST_IS_PUBLIC, True),
            val(properties.get(ST_LICENSE)),
            val(properties.get(ST_LICENSE_LOGO_URL)),
            val(properties.get(ST_METADATA_TEXT)),
            image_type,
            original_filename,
            val(properties.get(ST_REMARKS)),
            val(properties.get(ST_SCOPE_ID)),
            val(properties.get(ST_SCOPE_TYPE)),
            val(properties.get(ST_SUBJECT_ORIENTATION)),
            val(properties.get(ST_SUBTYPE)),
            val(properties.get(ST_TITLE)),
            val(properties.get(ST_TYPE)),
            agent_id
        )

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
            print(f"Error fetching attchment internal id: {internal_id}\n sql:{sql}")
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
