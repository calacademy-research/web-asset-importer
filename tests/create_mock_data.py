#!/usr/bin/env python3
import os
import json
import re
import shutil
from datetime import datetime

def scan_test_images_directory(directory_path="iz_test_images"):
    """
    Scan the iz_test_images directory and create a mock data structure
    that can be used in test_iz_importer.py
    """
    if not os.path.exists(directory_path):
        print(f"Error: Directory {directory_path} not found")
        return None
    
    mock_data = {
        "files": [],
        "directories": [],
        "metadata": {}
    }
    
    # Walk through the directory
    for root, dirs, files in os.walk(directory_path):
        # Add directory to our structure
        rel_path = os.path.relpath(root, start=os.path.dirname(directory_path))
        if rel_path != ".":
            mock_data["directories"].append(rel_path)
        
        # Process files
        for file in files:
            file_path = os.path.join(root, file)
            rel_file_path = os.path.join(rel_path, file)
            if rel_path == ".":
                rel_file_path = file
                
            # Skip hidden files
            if file.startswith('.'):
                continue
                
            # Get file extension
            _, ext = os.path.splitext(file)
            ext = ext.lower()
            
            # Check if it's an image file
            if ext in ['.jpg', '.jpeg', '.tif', '.tiff', '.png']:
                # Extract CASIZ number if present in filename
                casiz_match = re.search(r'casiz[_\s]*(\d+)', file.lower())
                conjunction_match = re.search(r'(\d{5,6}[_-]\d{5,6})', file)
                simple_number_match = re.search(r'(\d{5,6})', file)
                
                casiz_numbers = []
                if casiz_match:
                    casiz_numbers.append(int(casiz_match.group(1)))
                elif conjunction_match:
                    parts = re.split(r'[_-]', conjunction_match.group(1))
                    casiz_numbers = [int(part) for part in parts if part.isdigit()]
                elif simple_number_match:
                    casiz_numbers.append(int(simple_number_match.group(1)))
                
                # Check for key.csv in the same directory
                key_file = os.path.join(root, "key.csv")
                has_key_file = os.path.exists(key_file)
                
                # Add file info to our structure
                file_info = {
                    "path": rel_file_path,
                    "extension": ext,
                    "casiz_numbers": casiz_numbers,
                    "has_key_file": has_key_file,
                    "size": os.path.getsize(file_path),
                    "modified": datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                }
                
                mock_data["files"].append(file_info)
                
                # If there's a key file, read its contents
                if has_key_file:
                    try:
                        with open(key_file, 'r', encoding='utf-8') as f:
                            key_content = f.read()
                    except UnicodeDecodeError:
                        try:
                            # Try with Latin-1 encoding which can handle most byte values
                            with open(key_file, 'r', encoding='latin-1') as f:
                                key_content = f.read()
                        except Exception as e:
                            print(f"Error reading {key_file}: {e}")
                            key_content = f"[Error reading file: {str(e)}]"
                    mock_data["metadata"][rel_file_path] = key_content
    
    # Save the mock data to a JSON file
    output_file = "iz_test_images_mock_data.json"
    with open(output_file, 'w') as f:
        json.dump(mock_data, f, indent=2)
    
    print(f"Mock data saved to {output_file}")
    print(f"Found {len(mock_data['files'])} image files in {len(mock_data['directories'])} directories")
    
    return mock_data

if __name__ == "__main__":
    mock_data = scan_test_images_directory()
