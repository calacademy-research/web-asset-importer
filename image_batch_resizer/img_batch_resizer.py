

import os
from string_utils import remove_non_numerics
from PIL import Image
import shutil
import multiprocessing
import time

class ImageResizer:
    def __init__(self, source_dir, dest_dir, tmp_dir, subdir_name, min_bar, quality, max_size_kb, resize_to=None):

        """ImageResizer: class written to convert folders of .tiffs into jpegs of custom quality and size.
            args:
                source_dir: the root directory you want to convert images from
                dest_dir: the destination to which the compressed images are saved
                tmp_dir: the name of the temporary directory in which iterative image resizing is performed.
                subdir_name: the name of the subdirectory in any parent or batch directory to look for
                min_bar: the minimum image barcode number which the script is allowed to resize.
                quality: the level of compression you want to resize the image to max 95, min 1
                max_size_kb: the maximum image size at which to stop compression
                resize_to: default none if no change, used if you want to change the image dimensions
        """
        self.source_dir = source_dir
        self.dest_dir = dest_dir
        self.tmp_dir = tmp_dir
        self.subdir_name = subdir_name
        self.min_bar = min_bar
        self.quality = quality
        self.max_size_kb = max_size_kb
        self.resize_to = resize_to
        self.output_file_path = ""
        self.tmp_file_path = ""

    def resize_tiff_folders(self):
        """resize_tiff_folder: uses os.walk to find folders containing tiff files"""
        os.makedirs(self.tmp_dir, exist_ok=True)
        for root, dirs, files in os.walk(self.source_dir):
            if self.subdir_name not in dirs:
                continue

            tiff_folder = os.path.join(root, self.subdir_name)
            output_dir = os.path.join(self.dest_dir, os.path.basename(root), 'resized_jpg')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            self.process_tiffs(tiff_folder, output_dir)

    def process_tiffs(self, tiff_dir, output_dir):
        """process_tiffs: filters out non-tiff files and Cover sheets,
        as well as images that do not make the minimum barcode threshold, send images
        that pass to be compressed."""
        for file_name in os.listdir(tiff_dir):
            if file_name.endswith(".txt"):
                continue

            if not file_name.lower().endswith(('.tiff', '.tif')):
                continue

            if "Cover" in file_name:
                continue

            digits = remove_non_numerics(file_name)
            if not digits:
                continue

            barcode = int(digits)
            if barcode < self.min_bar:
                continue

            base_name = os.path.splitext(file_name)[0]
            output_file_name = f"{base_name}.jpg"
            self.output_file_path = os.path.join(output_dir, output_file_name)
            self.tmp_file_path = os.path.join(self.tmp_dir, output_file_name)

            if self.skip_existing_file(self.output_file_path):
                continue

            file_path = os.path.join(tiff_dir, file_name)
            img_quality = self.quality
            with Image.open(file_path) as image:
                print(f"resizing file {file_name}")

                if self.resize_to is not None:
                    image = image.resize(self.resize_to)

                self.compress_image_quality(image, img_quality, file_name)

    def compress_image_quality(self, image, img_quality, file_name):
        """iteratively processes each image such that it is compressed to the desired size,
        at maximum possible quality. Sets lower quality threshold at 20, below which
        color quality starts to noticeably break down."""
        while img_quality > 20:
            image.save(self.tmp_file_path, 'JPEG', quality=img_quality, optimize=True, subsampling=0)
            current_size_kb = os.path.getsize(self.tmp_file_path) / 1024  # Get current file size in KB

            if current_size_kb <= self.max_size_kb:
                shutil.copyfile(self.tmp_file_path, self.output_file_path)
                os.remove(self.tmp_file_path)
                print(f"Image {file_name} resized successfully")
                return

            if current_size_kb > (self.max_size_kb + 300):
                img_quality -= 5  # Decrease quality by 5 if over .3 MB above limit
            else:
                img_quality -= 1  # Decrease quality by 1 if under .3 MB above limit

        print(f"Warning: Could not reduce {file_name} to under {self.max_size_kb}KB without dropping below min quality.")

    def skip_existing_file(self, output_file_path):
        """Finds out if output file already exists and less than max size threshold"""
        if os.path.exists(output_file_path):
            size = os.path.getsize(output_file_path)
            if size / 1024 <= self.max_size_kb:
                print(f"Skipping {os.path.basename(output_file_path)}, already processed.")
                return True
        return False

    def run_with_restarts(self):
        """runs resizer using multiprocessing and will restart process on non-zero exit code to account for
        latency/rate limits on API for mounted drive."""
        print(f"Parent process PID: {os.getpid()}", flush=True)
        while True:
            process = multiprocessing.Process(target=self.resize_tiff_folders)
            process.start()
            process.join()
            exit_code = process.exitcode
            if exit_code == 0:
                break
            print(f"Script exited with code {exit_code}. Restarting in 5 minutes...", flush=True)
            time.sleep(300)

if __name__ == "__main__":
    source_directory = "/storage_01/picturae/delivery"
    dest_directory = "/admin/picturae_drive_mount/CAS_for_OCR"
    tmp_folder = "/admin/web-asset-importer/image_batch_resizer/tmp_resize"
    image_resizer = ImageResizer(source_directory, dest_directory, tmp_folder, "undatabased", 800000, 80, 999, (2838, 3745))
    image_resizer.run_with_restarts()
