"""metadata_tools: utility functions for the addition, removal and reading of iptc and exif image metadata"""
from timeout import timeout
import errno
import os
import logging
from iptcinfo3 import IPTCInfo
import exifread
import PIL
from PIL import Image, ExifTags
import subprocess

class MetadataTools:

    # Hangs on some files, don't know why, needs to be killed
    @timeout(20, os.strerror(errno.ETIMEDOUT))
    def __init__(self):

        self.logger = logging.getLogger('MetadataTools')

        self.logger.setLevel(logging.DEBUG)

    def is_file_larger_than(self, filepath: str, size_in_mb: int) -> bool:
        """
        Check if a file at the given filepath is larger than the specified size (in megabytes).
        """
        # Get the size of the file in bytes
        size_in_bytes = os.path.getsize(filepath)

        # Convert the size to megabytes
        size_in_mb_actual = size_in_bytes / (1024 * 1024)

        print(size_in_mb_actual)

        # Compare the actual size with the specified size
        return size_in_mb_actual > size_in_mb

    # def remove_iptc_fields(self, image_path, fields_to_remove):
    #     """removes specified iptc fields
    #         args:
    #             image_path
    #
    #         """
    #     with ExifTool() as et:
    #         et.execute(f"-IPTC:{','.join(fields_to_remove)}=")
    #         et.execute(f"-IPTC:{','.join([f'iptc:{field}' for field in fields_to_remove])}=")
    #
    #     print("IPTC fields removed successfully.")

    def iptc_attach_metadata(self, iptc_dict: dict, path: str):
        """Attach IPTC metadata to an image file."""
        info = self.read_iptc_metadata(path)
        if info is not None:
            for key, value in iptc_dict.items():
                info[key] = value
            try:
                info.save()
                self.logger.info("IPTC metadata attached successfully.")
            except Exception as e:
                self.logger.error(f"Error saving IPTC metadata: {str(e)}")
        else:
            raise ValueError("None Returned")

    def read_iptc_metadata(self, path: str):
        """reads iptc metadata of image and returns the dictionary"""
        info = IPTCInfo(path, force=True)
        return info

    def attach_exif_metadata(self, path, exif_dict):
        """attaches exif metadata tags to image using ExifTools subprocess in command line
        args:
            path: path to image
            exif_dict: dictionary of exif terms using exif codes, and new values to assign"""

        for exif_code, exif_value in exif_dict.items():
            exif_code = self.exif_code_to_tag(exif_code)
            command = ['exiftool', f"-{exif_code}={exif_value}", path]
            subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("exif added succesfully")


    # still doesn't work yet, but close
    def remove_exif_tags(self, path, exif_codes):
        """
        Remove specified EXIF tags from an image.
        args:
            path: The path to the image.
            exif_codes: A list of EXIF codes to remove.
        """
        try:
            # Load EXIF
            exif_data = self.read_exif_metadata(path=path, convert_tags=False)
            # Iterate over the tags to remove
            for code in exif_codes:
                if code in exif_data:
                    exif_data[code] = None

            self.attach_exif_metadata(path=path, exif_dict=exif_data)

            print("EXIF tags removed successfully")
        except Exception as e:
            print(f"Error removing EXIF tags: {e}")

    def exif_code_to_tag(self, exif_code):
        """converts exif code into the string of the tag name
            args:
                exif_code: the integer code of an exif tag to convert to TAG"""
        tag_name = exifread.tags.EXIF_TAGS.get(exif_code, "Unknown Tag")
        return tag_name[0]

    def read_exif_metadata(self, path, convert_tags=True):
        """reads and returns exif metadata, reads exif codes into TAG names
            args:
                path: path to image
                convert_tags: True to convert tags to string, False keep exif codes
        """
        img = Image.open(path)
        if convert_tags is True:
            exif = {
                PIL.ExifTags.TAGS[k]: v
                for k, v in img._getexif().items()
                if k in PIL.ExifTags.TAGS
            }
        else:
            exif = img._getexif()

        img.close()
        return exif


if __name__ == "__main__":
    # print("Running tests...")
    #
    md = MetadataTools()

    # # test code commented out
    #
    # md.iptc_attach_metadata(path='tests/test_images/test_image.jpg', iptc_dict={
    # 'by-line': "Mateo De La Roca",
    # 'copyright notice': "@test_copyright_KML",
    # 'caption/abstract': "An upsidedown image of a woodworking shop",
    # 'City': 'San York'})
    #
    # md.attach_exif_metadata(path='tests/test_images/test_image.jpg', exif_dict = {271: 'Hauwei'})
    # #
    # # # md.remove_iptc_fields(fields_to_remove
    # print(md.read_exif_metadata(path='tests/test_images/test_image.jpg', convert_tags=False))
    #
    # md.remove_exif_tags(path='tests/test_images/test_image.jpg', exif_codes=[271])
    #
    # print(md.read_exif_metadata(path='tests/test_images/test_image.jpg', convert_tags=False))

    # print(md.read_iptc_metadata(path='tests/test_images/test_image.jpg'))
    #
    # print(md.is_file_larger_than('tests/test_images/test_image.jpg', 1.2))