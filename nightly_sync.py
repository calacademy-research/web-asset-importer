from image_db import ImageDb
from attachment_utils import AttachmentUtils
from db_utils import DbUtils
from get_configs import get_config
import traceback
import logging
import sys
from typing import Optional, Callable

image_db: Optional[ImageDb] = None
attachment_utils: Optional[AttachmentUtils] = None
collection_object_redaction_checker: Optional[Callable] = None


def get_specify_state(internal_filename):
    global attachment_utils, collection_object_redaction_checker
    coid = attachment_utils.get_collectionobjectid_from_filename(internal_filename)
    if coid is None:
        return None
    redacted_collection_object = collection_object_redaction_checker(coid)
    redacted_attachment = attachment_utils.get_is_attachment_redacted(internal_filename)
    logging.debug(
        f"get specify state {internal_filename}, collection object: {coid} collection object state: {redacted_collection_object} attachment state: {redacted_attachment}")

    return redacted_collection_object or redacted_attachment


def redact(internal_filename, redacted):
    logging.debug("\n\n----------")
    logging.debug(f"Checking: {internal_filename} currently {redacted}")
    redacted_in_specify = get_specify_state(internal_filename)
    if redacted_in_specify is None:
        logging.warning(f"Cannot find collection object ID for {internal_filename}")
    elif redacted_in_specify != redacted:
        logging.debug(f"State change!")
        image_db.update_redacted(internal_filename, redacted_in_specify)
    else:
        logging.debug(f"No state change required. State is {redacted} object is {internal_filename}")


def do_sync(collection_name, specify_db_connection, co_redaction_method):
    global image_db, attachment_utils, collection_object_redaction_checker

    print(f"Starting sync for {collection_name}..")
    image_db = ImageDb()
    attachment_utils = AttachmentUtils(specify_db_connection)
    collection_object_redaction_checker = getattr(attachment_utils, co_redaction_method)
    cursor = image_db.get_cursor()
    query = f"""SELECT  internal_filename,  redacted FROM images where collection='{collection_name}'"""

    cursor.execute(query)
    record_list = []
    found_tuples = []
    for (internal_filename, redacted) in cursor:
        found_tuples.append((internal_filename, redacted))
    cursor.close()
    for (internal_filename, redacted) in found_tuples:
        next_record = False
        while next_record is False:
            try:
                redact(internal_filename, redacted)
                next_record = True
            except ReferenceError as e:
                print(f"Reference error, skipping: {e}", file=sys.stderr, flush=True)
                print(f"   internal filename: {internal_filename} redacted: {redacted}", file=sys.stderr, flush=True)
                next_record = True
            except Exception as e:
                print(f"Error, probably sql: \"{e}\"", file=sys.stderr, flush=True)
                print(f"exception type: {type(e).__name__}", file=sys.stderr, flush=True)

    return record_list


COLLECTION_CONFIG = {
    "Botany": {
        "config_key": "Botany",
        "co_redaction_method": "get_is_botany_collection_object_redacted",
    },
    "Ichthyology": {
        "config_key": "Ichthyology",
        "co_redaction_method": "get_is_botany_collection_object_redacted",
    },
    "IZ": {
        "config_key": "IZ",
        "co_redaction_method": "get_is_iz_collection_object_redacted",
    },
}


def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <collection>")
        print(f"Available collections: {', '.join(COLLECTION_CONFIG.keys())}")
        sys.exit(1)

    collection_arg = sys.argv[1]
    if collection_arg not in COLLECTION_CONFIG:
        print(f"Unknown collection: {collection_arg}")
        print(f"Available collections: {', '.join(COLLECTION_CONFIG.keys())}")
        sys.exit(1)

    config_entry = COLLECTION_CONFIG[collection_arg]
    importer_config = get_config(config=config_entry["config_key"])

    collection_name = importer_config.COLLECTION_NAME
    specify_db_connection = DbUtils(
        importer_config.USER,
        importer_config.PASSWORD,
        importer_config.SPECIFY_DATABASE_PORT,
        importer_config.SPECIFY_DATABASE_HOST,
        importer_config.SPECIFY_DATABASE)

    do_sync(collection_name, specify_db_connection, config_entry["co_redaction_method"])


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        traceback.print_exc()
