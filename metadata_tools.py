"""metadata_tools: utility functions for the addition, removal and reading of iptc and exif image metadata"""
import pandas as pd

from timeout import timeout
import errno
import os
import logging
from iptcinfo3 import IPTCInfo
import exifread
import PIL
from PIL import Image, ExifTags
import subprocess
import traceback


## warning:  standard python exif packages exif, and Pillow, exifread and piexif tool,
#            have a pattern of overwriting equivalent iptc fields, so a command line pipe
#            using exiftools is used instead to write exif data.

class MetadataTools:
    @timeout(20, os.strerror(errno.ETIMEDOUT))
    def __init__(self, path):
        self.path = path
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)

    def read_exif(self):
        """Reads all EXIF tags from an image using ExifTool with advanced formatting and returns them as a dictionary."""
        command = ['/usr/local/bin/exiftool', '-a', '-g', '-G', self.path]
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.stderr:
                raise ValueError(f"ExifTool error: {result.stderr.strip()}")
            tags = {}
            for line in result.stdout.split("\n"):
                if ": " in line:
                    group, key_value = line.split("]", 1)
                    key, value = key_value.split(":", 1)
                    formatted_group = group.replace('[', '').strip()
                    formatted_key = key.replace(' ', '').strip()
                    if value.strip():
                        tags[formatted_group + ':' + formatted_key] = value.strip()
            return tags
        except Exception as e:
            traceback.print_exc()
            raise ValueError(f"Command returned with error: {e}")
        finally:
            self.logger.info("EXIF data read successfully")

    def write_exif(self, exif_dict):
        """Writes all exif tags to an image with a single call to ExifTool"""
        self.logger.info(f"Processing EXIF data for: {self.path}")
        args = ["exiftool", "-overwrite_original"]
        args.extend([f"-{key}={value}" for key, value in exif_dict.items()])
        args.append(self.path)

        try:
            subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            traceback.print_exc()
            raise ValueError(f"ExifTool command returned with error: {e}")
        self.logger.info("EXIF data added successfully")



# if __name__ == "__main__":
#     # print("Running tests...")
#     #
#     md = MetadataTools(path='picturae_img/PIC_2023-06-28/CAS999999981.TIF')
#     md.()
#
#     # md.iptc_attach_metadata(iptc_field="copyright notice", iptc_value="test_val")
#     # print(md.read_iptc_metadata())
#     print(md.read_exif_metadata(convert_tags=False))



