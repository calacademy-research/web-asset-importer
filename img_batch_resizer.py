import os

from string_utils import remove_non_numerics
from PIL import Image
from gen_import_utils import read_json_config
config = read_json_config("Botany_PIC")
import re

source_dir = config['PREFIX'] + ['BOTANY_PREFIX']
def convert_tiff_folder(source_dir, quality, min_bar, resize_to=None):
    """convert_tiff_folder: creates new folder of resized jpegs from a
         folder of tiffs. Uses a walk perform recursively
         args:
            source_dir: the root directory you want to perform the operation on
            quality: the level of compression you want to resize the image to max 95, min 1
            resize_to: default none, used if you want to change the image dimensions
    """
    for root, dirs, files in os.walk(source_dir):
        if 'undatabased' in dirs:
            tiff_dir = os.path.join(root, 'undatabased')
            output_dir = os.path.join(root, 'resized_jpegs')

            # Create the output directory if it doesn't exist
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            for file_name in os.listdir(tiff_dir):
                barcode = int(remove_non_numerics(file_name))
                if barcode >= min_bar and "Cover" not in file_name:
                    if file_name.lower().endswith('.tiff') or file_name.lower().endswith('.tif'):
                        file_path = os.path.join(tiff_dir, file_name)
                        image = Image.open(file_path)

                        # Resize the image if resize_to is not None
                        if resize_to is not None:
                            image = image.resize(resize_to)

                        # Construct the output file name and path
                        base_name = os.path.splitext(file_name)[0]
                        output_file_name = f"{base_name}.jpg"
                        output_file_path = os.path.join(output_dir, output_file_name)

                        # Save the image as JPEG with the specified quality
                        image.save(output_file_path, 'JPEG', quality=quality, optimize=True, subsampling=0)


if __name__ == "__main__":
    convert_tiff_folder(source_dir, quality=76, min_bar=800000, resize_to=(2838, 3745))

