import json

def restructure_json(input_file, output_file):
    # Read the original JSON file
    with open(input_file, 'r') as f:
        data = json.load(f)

    # Get the mapping dictionaries
    casiz_filename = data.get('casiz_number_cases', {})
    casiz_exif = data.get('casiz_number_cases_exif', {})
    directory_matches = data.get('directory_matches', {})
    copyright_dir = data.get('copyright_from_directory', {})
    copyright_exif = data.get('copyright_from_exif', {})
    metadata = data.get('metadata', {})

    # Update each file entry with new keys
    files_map = {}
    for file in data['files']:
        files_map[file['path']] = file.copy()
        file_entry = files_map[file['path']]
        path = file['path']
        
        # Add casiz_from_filename
        file_entry['casiz'] = {}
        file_casiz_entry = file_entry['casiz']
        file_casiz_entry['from_filename'] = casiz_filename.get(path)
        
        # Add casiz_from_exif
        # Some paths in exif map have full path, so we need to handle both cases
        exif_value = casiz_exif.get(path) or casiz_exif.get(f"/Users/shiqi/Workspace/CAS/web-asset-importer/tests/iz_importer/../{path}")
        file_casiz_entry['from_exif'] = exif_value

        # Add casiz_from_directory
        # Same path handling as above
        dir_value = directory_matches.get(path) or directory_matches.get(f"/Users/shiqi/Workspace/CAS/web-asset-importer/tests/iz_importer/../{path}")
        file_casiz_entry['from_directory'] = dir_value

        # Add copyright_from_directory
        file_entry['copyright'] = {}
        file_copyright_entry = file_entry['copyright']
        file_copyright_entry['from_directory'] = copyright_dir.get(path)

        # Add copyright_from_exif
        file_copyright_entry['from_exif'] = copyright_exif.get(path)

        file_entry['metadata'] = {}
        file_metadata_entry = file_entry['metadata']
        if metadata.get(path):
            for metadata_info in metadata.get(path).split('\n'):
                if metadata_info:
                    try:
                        key, *value = metadata_info.split(',')
                        file_metadata_entry[key] = value
                    except:
                        print(f"Error splitting metadata: {metadata_info}")

    # Remove the old mapping dictionaries
    data.pop('casiz_number_cases', None)
    data.pop('casiz_number_cases_exif', None)
    data.pop('directory_matches', None)
    data.pop('copyright_from_directory', None)
    data.pop('copyright_from_exif', None)
    data.pop('metadata', None)
    # Write the restructured JSON
    data['files'] = files_map
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    input_file = "tests/iz_importer/iz_test_images_mock_data.json"
    output_file = "tests/iz_importer/iz_test_images_mock_data_restructured.json"
    restructure_json(input_file, output_file)