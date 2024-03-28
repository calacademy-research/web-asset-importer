import os

from string_utils import remove_non_numerics
from PIL import Image
import multiprocessing
# from get_configs import get_config
# config = get_config("Botany_PIC")
source_dir = "/admin/picturae_drive_mount/CAS_for_OCR"

def convert_tiff_folder(source_dir, quality, min_bar, max_size_kb, resize_to=None):
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
                        # Construct the output file name and path
                        base_name = os.path.splitext(file_name)[0]
                        output_file_name = f"{base_name}.jpg"
                        output_file_path = os.path.join(output_dir, output_file_name)

                        # Skip if output file already exists
                        if os.path.exists(output_file_path) is True:
                            size = os.path.getsize(output_file_path)
                            if size/1024 <= max_size_kb:
                                print(f"Skipping {file_name}, already processed.")
                                continue
                            else:
                                pass

                        file_path = os.path.join(tiff_dir, file_name)
                        img_quality = quality
                        # im
                        with Image.open(file_path) as image:

                            print(f"resizing file {file_name}")

                            # Resize the image if resize_to is not None
                            if resize_to is not None:
                                image = image.resize(resize_to)

                            # Save the image as JPEG with the specified quality
                            # Assuming this is the threshold size in KB

                            while img_quality > 20:
                                image.save(output_file_path, 'JPEG', quality=img_quality, optimize=True, subsampling=0)
                                current_size_kb = os.path.getsize(
                                    output_file_path) / 1024  # Get current file size in KB

                                if current_size_kb <= max_size_kb:
                                    print(f"Image {file_name} resized successfully")
                                    break
                                # Adjust quality decrease based on current image size
                                if current_size_kb > (max_size_kb + 300):
                                    img_quality -= 5  # Decrease quality by 5 if above 1.3 MB
                                else:
                                    img_quality -= 1  # Decrease quality by 1 if below 1.3 MB

                            if img_quality <= 20:
                                print(f"Warning: Could not reduce {file_name} to under {max_size_kb}KB "
                                      f"without dropping below min quality.")


if __name__ == "__main__":
    # ideal presets for picturae project, can change for other use cases
    p = multiprocessing.Process(target=convert_tiff_folder(source_dir, quality=80, min_bar=800000, max_size_kb=999,
                                                           resize_to=(2838, 3745)))
    p.start()
    p.join()
