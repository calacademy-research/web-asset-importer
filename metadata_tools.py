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

    def is_file_larger_than(self, size_in_mb: float) -> bool:
        """
        Check if a file at the given filepath is larger than the specified size (in megabytes).
        """
        # Get the size of the file in bytes
        size_in_bytes = os.path.getsize(self.path)

        # Convert the size to megabytes
        size_in_mb_actual = size_in_bytes / (1024 * 1024)

        self.logger.debug(f"{size_in_mb_actual}")

        # Compare the actual size with the specified size
        return size_in_mb_actual > size_in_mb


    def iptc_attach_metadata(self, iptc_field: str, iptc_value):
        """Attach IPTC metadata to an image file."""
        info = self.read_iptc_metadata()
        if info is not None:
            info[iptc_field] = iptc_value
            try:
                info.save()
                self.logger.info("IPTC metadata attached successfully.")
            except Exception as e:
                self.logger.error(f"Error saving IPTC metadata: {str(e)}")
        else:
            raise ValueError("None Returned")

    def read_iptc_metadata(self):
        """reads iptc metadata of image and returns the dictionary"""
        info = IPTCInfo(self.path, force=True)
        return info

    def exif_attach_metadata(self, exif_code: int, exif_value):
        """attaches exif metadata tags to image using ExifTools subprocess in command line
        args:
            path: path to image
            exif_dict: dictionary of exif terms using exif codes, and new values to assign
        """
        if not pd.isna(exif_code):
            exif_tag = self.exif_code_to_tag(exif_code)
            command = ['exiftool', '-overwrite_original', f'-{exif_tag}="{exif_value}"', self.path]
            try:
                subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except Exception as e:
                traceback.print_exc()
                raise ValueError(f"command return with error  {e}")
            self.logger.info("exif added succesfully")
        else:
            logging.warning("No exif code supplied, skipping attach exif")


    def exif_code_to_tag(self, exif_code):
        """converts exif code into the string of the tag name
            args:
                exif_code: the integer code of an exif tag to convert to TAG"""
        exif_code = int(exif_code)
        tag_name = exifread.tags.EXIF_TAGS.get(exif_code, "Unknown Tag")

        if tag_name == "Unknown Tag":
            raise ValueError("unknown code")
        else:
            return tag_name[0]

    def read_exif_metadata(self, convert_tags=True):
        """reads and returns exif metadata, reads exif codes into TAG names
            args:
                path: path to image
                convert_tags: True to convert tags to string, False keep exif codes
        """
        try:
            img = Image.open(self.path)
        except Exception as e:
            logging.warning(f"Unidentified image error in metadata extraction: {self.path} : {e}")
            return None

        if convert_tags is True:
            exif = {
                PIL.ExifTags.TAGS[k]: v
                for k, v in img.getexif().items()
                if k in PIL.ExifTags.TAGS
            }
        else:
            exif = img.getexif()

        img.close()
        return exif

    def write_exif_tags(self, exif_dict):
        """iterates through exif dict keys and values to attach them to image"""
        self.logger.info(f"processing exif ring for filepath: {self.path}")
        for key, value in exif_dict.items():
            key = int(key)
            self.exif_attach_metadata(exif_code=key, exif_value=value)


# if __name__ == "__main__":
#     # print("Running tests...")
#     #
#     md = MetadataTools(path='picturae_img/PIC_2023-06-28/CAS999999981.TIF')
#     md.()
#
#     # md.iptc_attach_metadata(iptc_field="copyright notice", iptc_value="test_val")
#     # print(md.read_iptc_metadata())
#     print(md.read_exif_metadata(convert_tags=False))



