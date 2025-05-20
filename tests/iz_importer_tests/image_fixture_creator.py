import subprocess
import os
import sys
import json
from PIL import Image
from datetime import datetime

def extract_exiftool_json(path):
    """Use exiftool to extract metadata as JSON"""
    result = subprocess.run(
        ["exiftool", "-j", "-G", "-n", path],
        stdout=subprocess.PIPE,
        check=True,
        text=True
    )
    return json.loads(result.stdout)[0]

def reduce_image_preserve_all_metadata(input_path, output_path=None, json_out_path="metadata.json"):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"File not found: {input_path}")

    ext = os.path.splitext(input_path)[1].lower()
    if ext not in [".jpg", ".jpeg", ".png", ".tiff", ".webp"]:
        raise ValueError(f"Unsupported format: {ext}")


    # Define output path
    if not output_path:
        base, _ = os.path.splitext(input_path)
        output_path = base + "_fixture" + ext
    # Step 1: Create 1x1 white image directly at the output path
    img = Image.new("RGB", (1, 1), (255, 255, 255))
    img.save(output_path, format=ext.lstrip(".").upper() if ext != ".jpg" else "JPEG")

    # Step 2: Copy all metadata from original into output image
    subprocess.run([
        "exiftool",
        "-overwrite_original",
        "-TagsFromFile", input_path,
        "-all:all",
        "-icc_profile",
        output_path
    ], check=True)


    # Get file stats
    file_stat = os.stat(output_path)
    size = file_stat.st_size
    modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat(timespec="seconds")

    # Extract EXIF metadata using exiftool
    exif_data = extract_exiftool_json(output_path)

    # Build the structured metadata dictionary
    metadata_entry = {
        input_path: {
            "path": input_path,
            "extension": ext,
            "has_key_file": False,
            "size": size,
            "modified": modified,
            "casiz": {
                "from_filename": None,
                "from_exif": [int(exif_data.get("EXIF:ImageUniqueID", "0"))] if "EXIF:ImageUniqueID" in exif_data else [],
                "from_directory": None,
                "from": "EXIF"
            },
            "copyright": {
                "from_directory": None,
                "from_exif": exif_data.get("XMP:Copyright", "")
            },
            "metadata": {
                "CopyrightDate": [exif_data.get("XMP:CopyrightDate", "")],
                "CopyrightHolder": [exif_data.get("XMP:Copyright", "")],
                "Credit": [exif_data.get("XMP:Credit", "")],
                "License": [exif_data.get("XMP:License", "")],
                "Remarks": [exif_data.get("XMP:Instructions", "")],
                "IsPublic": [exif_data.get("XMP:Marked", "")],
                "subType": [exif_data.get("XMP:Subject", "")],
                "createdByAgent": [exif_data.get("XMP:Creator", "")],
                "metadataText": [exif_data.get("XMP:Credit", "")],
                "remove": ["true"]
            },
            "copyright_source": "file key"
        }
    }

    # Write or update JSON file
    if os.path.exists(json_out_path):
        with open(json_out_path, "r") as f:
            existing = json.load(f)
    else:
        existing = {}

    existing.update(metadata_entry)

    with open(json_out_path, "w") as f:
        json.dump(existing, f, indent=2)

    print(f"Reduced image saved to: {output_path}")
    print(f"Metadata written to: {json_out_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python reduce_image_with_metadata.py <image_path>")
        sys.exit(1)
    
    reduce_image_preserve_all_metadata(sys.argv[1])
