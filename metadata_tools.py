
from timeout import timeout
import errno
import os
import logging
from iptcinfo3 import IPTCInfo
import binascii
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

    def is_file_larger_than(self, filepath: str, size_in_mb) -> bool:
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

    # def remove_iptc_fields(image_path, fields_to_remove):
    #     """removes specified iptc fields"""
    #     with ExifTool() as et:
    #         et.execute(f"-IPTC:{','.join(fields_to_remove)}=")
    #         et.execute(f"-IPTC:{','.join([f'iptc:{field}' for field in fields_to_remove])}=")
    #
    #     print("IPTC fields removed successfully.")

    def iptc_attach_metadata(self, iptc_dict, path):
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

    def read_iptc_metadata(self, path):
        """reads iptc metadata of image and returns the info"""
        info = IPTCInfo(path, force=True)
        return info


    def attach_exif_metadata(self, path, exif_dict):
        print(path)

        for exif_code, exif_value in exif_dict.items():
            exif_code = self.exif_code_to_tag_name(exif_code)
            command = ['exiftool', f"-{exif_code}={exif_value}", path]
            subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("exif added succesfully")

    def exif_code_to_tag_name(self, exif_code):
        # Use the exifread library to get the tag name
        tag_name = exifread.tags.EXIF_TAGS.get(exif_code, "Unknown Tag")
        return tag_name[0]

    def read_exif_metadata(self, path):
        """reads and returns exif metadata"""
        img = Image.open(path)
        exif = {
            PIL.ExifTags.TAGS[k]: v
            for k, v in img._getexif().items()
            if k in PIL.ExifTags.TAGS
        }
        img.close()
        return exif

    import binascii

    def write_metadata_to_txt(self, image_path, output_file):
        try:
            # Open the image file
            image = Image.open(image_path)

            # Get the metadata as bytes (Exif data)
            metadata_bytes = image.info.get("exif", b"")

            # Perform the hex dump
            hex_dump = binascii.hexlify(metadata_bytes).decode('utf-8')

            # Save the hex dump to the specified text file
            with open(output_file, 'w') as output:
                output.write(hex_dump)

            print(f"Metadata hex dump saved to {output_file}")
        except Exception as e:
            print("Error:", str(e))


if __name__ == "__main__":
    # print("Running tests...")
    #
    md = MetadataTools()

    md.iptc_attach_metadata(path='tests/test_images/test_image.jpg', iptc_dict={
    'by-line': "Mateo De La Roca",
    'copyright notice': "@test_copyright_KML",
    'caption/abstract': "An upsidedown image of a woodworking shop",
    'City': 'San York'})

    md.attach_exif_metadata(path='tests/test_images/test_image.jpg', exif_dict = {271: 'Samsung'})

    # md.remove_iptc_fields(fields_to_remove
    print(md.read_exif_metadata(path='tests/test_images/test_image.jpg'))
    print(md.read_iptc_metadata(path='tests/test_images/test_image.jpg'))

    print(md.is_file_larger_than('tests/test_images/test_image.jpg', 1.2))

    # md.write_metadata_to_txt(image_path='tests/test_images/test_image.jpg', output_file='tests/test_images/test_hexdump.txt')