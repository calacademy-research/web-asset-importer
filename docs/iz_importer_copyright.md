# 🧭 Copyright Extraction Workflow – IZ Importer

This document describes the step-by-step **workflow used by the IZ Importer** to extract copyright information for a given file using a prioritized strategy. It checks multiple potential sources in order of reliability and returns a string indicating the source of the copyright, if found.


## Extraction Sources

- **`orig_case_full_path`** (`str`):  
  The full path to the original file. Used to derive the directory and filename for fallback strategies.

- **`exif_metadata`** (`dict` or `None`):  
  A dictionary of EXIF metadata. Potential source of copyright information.

- **`file_key`** (`dict` or `None`):  
  A dictionary possibly containing a `'CopyrightHolder'` key. Considered the most trusted source. The content of the dictionary is provided from the `key.csv` file under the same directory for the image file or under the nearest ancestor directory.

---

## Logic Flow

1. ### Check `file_key`
   - If `file_key` is provided and contains a non-`None` `'CopyrightHolder'` value, this value is used.
   - This is the most authoritative source.

2. ### Check EXIF Metadata
   - If EXIF metadata is available, the function delegates to `get_copyright_from_exif`. (see helper function for more details)
   - If the result is valid and not the generic placeholder `"copyright"`, it is used.

3. ### Check Directory-Level Metadata
   - Uses the directory path of the original file and checks for metadata that might be stored at the folder level.
   - This check is delegated to `attempt_directory_copyright_extraction`.

4. ### Check Filename
   - As a last resort, attempts to parse copyright information directly from the filename using `extract_copyright_from_string`.

5. ### Fallback
   - If none of the above yield results, returns `None`.

---

## Helper Functions

### `get_copyright_from_exif`

This helper function attempts to extract copyright information from the provided EXIF metadata.

1. **Search Keys**: The function checks for two specific keys in the metadata:
   - `"EXIF:Copyright"`
   - `"IPTC:CopyrightNotice"`
2. **Data Cleaning and Validation**:
   - If either key is present, the value is stripped of leading/trailing whitespace.
   - If the string starts with a stray character like `Â`, that character is removed.
   - If the resulting string is very short (length ≤ 2) or contains a sequence of null bytes (`\x00\x00\x00\x00\x00\x00\x00`), it is considered invalid, and the function returns `None`.

---

### `attempt_directory_copyright_extraction`

This helper function attempts to extract copyright information by inspecting the names of the directories in the provided path.

1. **Directory Split**: The path is split into its individual directories using the `'/'` separator.
2. **Reverse Iteration**: iterates through the directories in reverse order, starting from the deepest nested folder up to the top-level.
3. **Copyright Extraction**: For each directory name, it utilize the same `extract_copyright_from_string` to extract copyright info

---

### `extract_copyright_from_string`

This helper function attempts to extract a copyright string from a given input string, typically a filename or directory name.

1. **Case-Insensitive Search**:
   - Converts the input string to lowercase to search for the word `'copyright'`.
   - Also searches for the literal copyright symbol `'©'`.
2. **Determine Which Keyword Occurs Last**:
   - If neither is found, returns `None`.
   - If `'copyright'` appears later than `'©'`, extracts the substring that follows `'copyright'`.
   - Otherwise, extracts the substring that follows `'©'`.
3. **Cleanup**:
   - Trims leading and trailing whitespace.
   - Removes any trailing substring that starts with an underscore (e.g., "_v1", "_draft").

---
