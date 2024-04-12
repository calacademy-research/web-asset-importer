#!/usr/bin/env python3
import argparse
import re
from get_configs import get_config
import os
import logging
import collection_definitions
from botany_importer import BotanyImporter
from picturae_importer import PicturaeImporter
from iz_importer import IzImporter
import sys
from ichthyology_importer import IchthyologyImporter
from image_client import ImageClient
from botany_purger import BotanyPurger
from PIC_undo_batch import PicturaeUndoBatch
from PIC_database_updater import UpdatePICFields
from BOT_database_updater import UpdateBotDbFields
args = None
logger = None


def parse_command_line():
    parser = argparse.ArgumentParser(
        description=f"""
             Tool to manipulate images on the CAS image server. 
             
             Available collections:
             {[x for x in collection_definitions.COLLECTION_DIRS.keys()]}
             
             Commands: import, search, purge. Collection is mandatory.
             """,
        formatter_class=argparse.RawTextHelpFormatter, add_help=True)

    parser.add_argument('-v', '--verbosity',
                        help='verbosity level. repeat flag for more detail',
                        default=0,
                        dest='verbose',
                        action='count')

    parser.add_argument('collection', help='Collection')

    subparsers = parser.add_subparsers(help='Select search or import mode', dest="subcommand")
    search_parser = subparsers.add_parser('search')
    import_parser = subparsers.add_parser('import')
    purge_parser = subparsers.add_parser('purge')
    update_parser = subparsers.add_parser('update')

    search_parser.add_argument('term')

    parser.add_argument('-d', '--date', nargs="?", help='batch date in the form YYYYMMDD, the date which the batch was imaged.', default=None)

    parser.add_argument('-m', '--md5', nargs="?", type=str,  help='md5 batch to remove from database', default=None)

    parser.add_argument('-f', '--full_import', nargs="?", type=bool, help='Set to True if doing an '
                                                                           'import that imports both data and images',
                        default=False)
    parser.add_argument('-e', '--existing_barcodes', nargs="?", type=bool, help="if True, skips creating skeleton"
                                                                                "record and creating new image record"
                                                                                "for images without collection objects",
                                                                            default=False)

    parser.add_argument('-uf', '--force_update', nargs="?", type=bool, help='Set to True if '
                                                                            'you desire to overwrite '
                                                                            'existing data when updating', default=False)

    return parser.parse_args()


def main(args):
    # clearing import logs
    if args.subcommand == 'search':
        image_client = ImageClient()
    elif args.subcommand == 'import':
        if args.collection == "Botany":
            bot_config = get_config(config="Botany")
            # get paths here
            paths = []
            for cur_dir in bot_config.BOTANY_SCAN_FOLDERS:
                paths.append(os.path.join(bot_config.PREFIX,
                                          bot_config.COLLECTION_PREFIX,
                                          cur_dir))
                print(f"Scanning: {cur_dir}")
            full_import = args.full_import
            BotanyImporter(paths=paths, config=bot_config, full_import=full_import)
        elif args.collection == 'Botany_PIC':
            pic_config = get_config(config="Botany_PIC")
            existing_barcodes = args.existing_barcodes
            if existing_barcodes:
                paths = []
                full_import = args.full_import

                for root, dirs, files in os.walk(pic_config.PREFIX):
                    if 'databased' in dirs:
                        img_dir = os.path.join(root, 'databased')
                        paths.append(img_dir)

                BotanyImporter(paths=paths, config=pic_config, full_import=full_import,
                               existing_barcodes=existing_barcodes)
            else:
                date_override = args.date

                # if none reverts to default, to get date of most recent folder in csv folder
                if date_override is None:
                    raise AttributeError("date argument missing from command line")

                scan_folder = re.sub(pattern=pic_config.FOLDER_REGEX, repl=f"_{date_override}_",
                                     string=pic_config.PIC_SCAN_FOLDERS)

                scan_folder = os.path.join(scan_folder, f"undatabased{os.path.sep}")

                paths = []
                full_dir = os.path.join(pic_config.PREFIX,
                                        scan_folder)
                paths.append(full_dir)

                PicturaeImporter(paths=paths, config=pic_config, date_string=date_override)


        elif args.collection == "Ichthyology":
            full_import = args.full_import
            IchthyologyImporter(full_import=full_import)
        elif args.collection == "IZ":
            full_import = args.full_import
            IzImporter(full_import=full_import)
    elif args.subcommand == 'purge':
        logger.debug("Purge!")

        if args.collection == "Botany":
            purger = BotanyPurger()
            purger.purge()
        if args.collection == "Botany_PIC":
            md5_insert = args.md5
            PicturaeUndoBatch(MD5=md5_insert)
    elif args.subcommand == 'update':
        if args.collection == "Botany_PIC":
            pic_config = get_config(config="Botany_PIC")
            date_override = args.date
            force_update = args.force_update
            UpdatePICFields(config=pic_config, date=date_override, force_update=force_update)
        if args.collection == 'Botany':
            bot_config = get_config(config="Botany")
            date_override = args.date
            force_update = args.force_update
            UpdateBotDbFields(config=bot_config, date=date_override, force_update=force_update)

    else:
        print(f"Unknown command: {args.subcommand}")



def setup_logging(verbosity: int):
    """
    Set the logging level, between 0 (critical only) to 4 (debug)

    Args:
        verbosity: The level of logging to set

    """
    global logger
    print("setting up logging...")
    logger = logging.getLogger('Client')
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(name)s — %(levelname)s — %(funcName)s:%(lineno)d - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # with this pattern, it's rarely necessary to propagate the error up to parent

    logger.propagate = False

    if verbosity == 0:
        logger.setLevel(logging.CRITICAL)
    elif verbosity == 1:
        logger.setLevel(logging.ERROR)
    elif verbosity == 2:
        logger.setLevel(logging.WARN)
    elif verbosity == 3:
        print(f"Logging level set to info...")

        logger.setLevel(logging.INFO)
    elif verbosity >= 4:
        print(f"Logging level set to full debug...")
        logger.setLevel(logging.DEBUG)


def bad_collection():
    if len(sys.argv) == 1:
        print(f"No arguments specified; specify a collection")
    else:
        print(f"Invalid collection: {sys.argv[1]}")
    print(f"Available collections: {[x for x in collection_definitions.COLLECTION_DIRS.keys()]}")
    print(f"Run {sys.argv[0]} --help for more info.")
    print("   Note: command and collection required for detailed help. e.g.:")
    print("   ./image-client.py Botany import --help")
    sys.exit(1)


if __name__ == '__main__':
    # if len(sys.argv) == 1 or sys.argv[1] not in collection_definitions.COLLECTION_DIRS.keys():
    #     bad_collection()
    args = parse_command_line()
    # if args.disable_test:
    #     print("RUNNING IN TEST ONLY MODE. See help to disable.")

    setup_logging(args.verbose)

    logger.debug(f"Starting client...")

    if args.collection not in collection_definitions.COLLECTION_DIRS.keys():
        bad_collection()

    main(args)
