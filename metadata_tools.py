
from timeout import timeout
import errno
import os
import iz_importer_config
import logging
import piexif
from PIL import Image
from iptcinfo3 import IPTCInfo
from exiftool import ExifTool
import picturae_config


class MetadataTools:

    # Hangs on some files, don't know why, needs to be killed
    @timeout(20, os.strerror(errno.ETIMEDOUT))
    def __init__(self, full_path, config):
        # self.decoded_exif_data = {}
        self.config = config
        # self.copyright = None
        # self.casiz_number = None

        self.logger = logging.getLogger('MetadataTools')

        self.logger.setLevel(logging.DEBUG)

        self.full_path = full_path

        if self.is_file_larger_than(full_path, 100):
            print("Larger than 100M, skipping IPTC extraction")

    def is_file_larger_than(self, filepath: str, size_in_mb) -> bool:
        """
        Check if a file at the given filepath is larger than the specified size (in megabytes).
        """
        # Get the size of the file in bytes
        size_in_bytes = os.path.getsize(filepath)

        # Convert the size to megabytes
        size_in_mb_actual = size_in_bytes / (1024 * 1024)

        # Compare the actual size with the specified size
        return size_in_mb_actual > size_in_mb


    # might not want to use a config dictionary

    # def iptc_clear_metadata(self):
    #     info = IPTCInfo(self.full_path, force=True)
    #     remove_list = ['contact', 'keywords']
    #
    #     for key in remove_list:
    #         = []
    #     info.save()

    def remove_iptc_fields(image_path, fields_to_remove):
        """removes specified iptc fields"""
        with ExifTool() as et:
            et.execute(f"-IPTC:{','.join(fields_to_remove)}=")
            et.execute(f"-IPTC:{','.join([f'iptc:{field}' for field in fields_to_remove])}=")

        print("IPTC fields removed successfully.")

    def iptc_attach_metadata(self):
        """Attach IPTC metadata to an image file."""
        info = IPTCInfo(self.full_path, force=True)
        for key, value in self.config.IPTC_Categories.items():
            info[key] = value

        info.save()

    def read_iptc_metadata(self):
        """reads iptc metadata of image and returns the info"""
        info = IPTCInfo(self.full_path, force=True)
        return info

    def attach_exif_metadata(self):
        """exif_attach_metadata: attaches new exif fields to exif metadata,
            using dictionary defined in config file.
        """
        metadata = self.config.EXIF_DECODER_RING
        img = Image.open(self.full_path)
        exif_data = img.getexif() or {}
        if exif_data is not None:
            exif_data.update(metadata)
        else:
            exif_data = metadata

        img.save(self.full_path, exif=exif_data)

    def read_exif_metadata(self):
        """reads and returns exif metadata"""
        image = Image.open(self.full_path)
        raw_exif_data = image.getexif()
        return raw_exif_data



    # def test_print_all_iptc(self):
    #     print(f"reading test data from {self.full_path}")
    #     image = Image.open(self.full_path)
    #     self.raw_exif_data = image.getexif()
    #     iptc_data_iptc = image.info.get("iptc", {})
    #     print(f"iptc: {iptc_data_iptc}")
    #     for cur_key in list(image.info.keys()):
    #         # Get the IPTC data
    #         iptc_data = image.info[cur_key]
    #
    #         print("key:" + cur_key)
    #         # Print the IPTC data
    #         if isinstance(iptc_data, dict):
    #             for tag, value in iptc_data.items():
    #                 try:
    #                     decoded_data = value.decode('utf-8')
    #                     return decoded_data
    #                 except AttributeError:
    #                     decoded_data = value
    #                 print(f"{TAGS.get(tag, tag)}: {decoded_data}")
    #         else:
    #
    #             print("Type of binary object: ", type(iptc_data))
    #
    #             # Get the length of the binary object
    #             # print("Length of binary object: ", len(iptc_data))
    #
    #     self.decode_exif_data()
    #     self.process_exif_casiz_elements()

    # def test_write_img_alsobroken(self):
    #     # Open the image file in binary mode
    #     image_path = self.full_path
    #     with open(image_path, "rb+") as image_file:
    #         # Read the binary data of the image file
    #         image_data = bytearray(image_file.read())
    #
    #         # Define the IPTC tag to add
    #         iptc_tag = "Keywords"
    #         iptc_value = "tag1, tag2, tag3"  # Multiple tags separated by commas
    #
    #         # Convert the IPTC tag and value to bytes
    #         iptc_tag_bytes = bytes(iptc_tag, encoding="utf-8")
    #         iptc_value_bytes = bytes(iptc_value, encoding="utf-8")
    #
    #         # Calculate the length of the IPTC tag and value
    #         iptc_tag_length = len(iptc_tag_bytes)
    #         iptc_value_length = len(iptc_value_bytes)
    #
    #         # Write the IPTC tag and value to the image data
    #         image_data.extend(b"\x1C" + iptc_tag_bytes + b"\x1E" + iptc_value_bytes)
    #
    #         # Update the length of the IPTC tag and value in the image data
    #         image_data[6] = iptc_tag_length >> 8
    #         image_data[7] = iptc_tag_length & 0xFF
    #         image_data[8] = iptc_value_length >> 8
    #         image_data[9] = iptc_value_length & 0xFF
    #
    #         # Seek back to the beginning of the image file and write the updated image data
    #         image_file.seek(0)
    #         image_file.write(image_data)

    # def test_write_img_broken(self):
    #
    #     # Open the image
    #     image = Image.open(self.full_path)
    #
    #     # Update EXIF metadata
    #     exif_data = image._getexif()
    #     if exif_data is None:
    #         exif_data = {}
    #     exif_data[TAGS.get("Copyright")] = "joe russack"
    #     # image.save("test.jpg", exif=exif_data)
    #
    #     # Update IPTC metadata
    #     iptc_data = image.info.get("iptc", {})
    #     print("before writing, iptc data return as type ", type(iptc_data))
    #
    #     if "keywords" not in iptc_data:
    #         iptc_data["keywords"] = []
    #
    #     iptc_data["keywords"] = ["joe russack"]
    #     iptc_data["photoshop"] = ["photoshop test"]
    #     iptc_data["iptc"] = ["photoshop test"]
    #     print(f"Wrote test data to {self.full_path}")
    #     # image.save(self.full_path, **iptc_data)
    #     image.save(self.full_path, iptc=iptc_data)

    def test_write_img_a(self):
        image = Image.open(self.full_path)
        raw_exif_data = image.getexif()

        print(raw_exif_data)

        # Add IPTC tags
        raw_exif_data["0th"] = [piexif.ImageIFD.Copyright]

        print(raw_exif_data)

        raw_exif_data["0th"][piexif.ImageIFD.Copyright] = "tag1, tag2, tag3"
        # exif_data["0th"][piexif.ImageIFD.Byline] = "Your Name"
        # exif_data["0th"][piexif.ImageIFD.Caption] = "Your caption"
        # Add more IPTC tags as needed

        # Save the image with the updated IPTC info
        exif_bytes = piexif.dump(raw_exif_data)
        piexif.insert(exif_bytes, self.full_path)
    #
    # def test_write_img_b(self):
    #     # Load the image
    #     image_path = self.full_path
    #     image = py3exiv2.Image(image_path)
    #
    #     # Add IPTC tags
    #     image["Iptc.Application2.Keywords"] = "tag1, tag2, tag3"
    #     image["Iptc.Application2.Byline"] = "Your Name"
    #     image["Iptc.Application2.Caption"] = "Your caption"
    #     # Add more IPTC tags as needed
    #
    #     # Save the image with the updated IPTC info
    #     image.save()


if __name__ == "__main__":
    print("Running tests...")
    # md = MetadataTools('/Users/joe/web-asset-server/image_client/iptc_exif_test.jpg')

    md = MetadataTools('tests/test_images/test_image.jpg', config=picturae_config)

    md.iptc_attach_metadata()

    # fields_to_remove = ["SupplementalCategories", "Keywords", "Contact"]  # List of IPTC fields to remove

    # md.remove_iptc_fields(fields_to_remove)

    md.attach_exif_metadata()

    print(md.read_iptc_metadata())
    print(md.read_exif_metadata())