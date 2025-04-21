# 🧭 CASIZ Extraction Workflow – IZ Importer

This document describes the step-by-step **workflow used by the IZ Importer** to find CASIZ IDs from an image. The process follows a logical fallback strategy that attempts to extract CASIZ identifiers from various metadata sources like the filename, EXIF data, and directory name.

---

## 🧱 High-Level Structure

The CASIZ ID extraction starts from the top-level `get_casiz_ids` method and goes through the following stages:

---

## ✅ Matching Success

At **any stage**, if a valid CASIZ number is extracted, it is stored in `self.casiz_numbers` and the workflow **short-circuits**, skipping the remaining steps.

---

## ❌ Matching Failure

If all three stages fail:
- `get_casiz_ids()` returns `False`
- No CASIZ ID is stored or associated with the image

---

## 🧩 Step-by-Step Workflow

### 1. `get_casiz_ids(image_path)`

This is the main entry point for CASIZ extraction. It performs a **sequential attempt** to find CASIZ data from three potential sources by the following order:

- First it will look into the **image filename**
- Then it will look into the **EXIF metadata**
- Last it will look into the **directory name**

> ⚠️ **Important:**  
> The process stops immediately once a match is found in any step.  
> For example, **if a CASIZ ID is found in the filename, the EXIF and directory steps are skipped.**

For each of the step above, IZ importer will try to do a two step match to extract the casiz number

---

### 2.1 📁 `attempt_filename_match`

This function checks whether the **filename** contains any recognizable CASIZ patterns.

---

### 2.2. 🧾 `get_casiz_from_exif`

If the filename match fails, this function tries to extract a CASIZ ID from the image’s **EXIF metadata** (e.g., description or title fields).

It will try to extract casiz from the following fields: (see metadata repo for more details on those constant definitions)

|EXIFConstants.IPTC_KEYWORDS|
|EXIFConstants.XMP_DC_SUBJECT|
|EXIFConstants.XMP_LR_HIERARCHICAL_SUBJECT|
|EXIFConstants.IPTC_CAPTION_ABSTRACT|
|EXIFConstants.XMP_DC_DESCRIPTION|
|EXIFConstants.EXIF_IFD0_IMAGE_DESCRIPTION|
|EXIFConstants.XMP_TITLE|

---

### 2.3 📂 `attempt_directory_match`

If both filename and EXIF methods fail, the importer attempts to extract a CASIZ ID from the **directory name** containing the image.


---

## 1. 🧩 FILENAME_CONJUNCTION_MATCH

```python
FILENAME_CONJUNCTION_MATCH = rf'(({CASIZ_MATCH})|([ ]*(and|or)[ ]*({CASIZ_MATCH})))+'
```

✅ Matches:
- Multiple CASIZ numbers in a filename
- Joined by and / or (case-sensitive)
- Handles:
  - casiz_12345 and cas_56789.jpg
  - 12345 and 67890.png

✅ Behavior:
- If matched:
  - Finds all occurrences
  - Extracts all digit sequences (e.g., 12345, 67890)
  - Stores unique integers in self.casiz_numbers
  - Sets self.title to the base filename (excluding extension)

---

## 2. 🔎 CASIZ_MATCH

```python
CASIZ_MATCH = rf'({CASIZ_PREFIX}{CASIZ_NUMBER_SHORT})|({CASIZ_NUMBER})'
```

✅ Matches:
- Prefixed forms like:
  - cas_123
  - casiz_test_123
  - cas_abc_123456
  - cas_abc_1234
  - cas_x-1234
  - cas1234
- Unprefixed numeric strings:
  - 12345, 6789012 (within 5 to 10 digits)

✅ Behavior:
- If matched:
  - Calls extract_exact_casiz_match(input_string)
  - If successful, saves as a single CASIZ number
  - For longer sequences, truncates to maximum allowed length (e.g., "cas 123456789012test" becomes "cas 1234567890")

❌ Does not match:
- Prefixes shorter than "cas" (e.g., "ca 125")
- Numbers with fewer than 3 digits after prefix (e.g., "cas 1")
- Standalone numbers with fewer than 5 digits (e.g., "12")
- Numbers without proper prefix format (e.g., "image123 _something-4444 file999")

---

## 2.1 🔍 CASIZ_NUMBER_EXACT

```python
CASIZ_NUMBER_EXACT = rf'({CASIZ_PREFIX}[^a-zA-Z0-9]*)(\d+)'
```

✅ Matches:
- Prefixed forms with digits immediately following:
  - casiz12345
  - casiz_sample_67890
  - test casiz_image-54321
  - casiz_image-543
  - image casiz_54321
  - abc_casiz_label# 88888
  - casiz path test 99999
  - CaSiZ_image_12345 (case insensitive)

✅ Behavior:
- If matched:
  - Extracts only the numeric part after the prefix
  - Returns the numeric part as a match group

❌ Does not match:
- Prefixes without digits (e.g., "casiz-only", "casiz_image")
- Paths with backslashes (e.g., "casiz path\\_99999")
- Paths with forward slashes (e.g., "casiz/image_dir_00001")
- Prefixes with special characters (e.g., "casiz©-77777")

---

## 🧠 Constants Reference

| Constant | Description |
|----------|-------------|
| MINIMUM_ID_DIGITS | Minimum digits allowed in a CASIZ ID (typically 5) |
| MAXIMUM_ID_DIGITS | Maximum digits allowed in a CASIZ ID (typically 10–12) |
| CASIZ_PREFIX | Prefix like cas, casiz, with optional symbols or spaces |
| CASIZ_MATCH | Regex for a general CASIZ match (prefixed or just digits) |
| CASIZ_NUMBER_SHORT | Shorter version used for prefix matching (≥3 digits) |
| FILENAME_CONJUNCTION_MATCH | Regex for matching multiple CASIZ IDs with "and"/"or" |

---

## 🧪 Example Inputs & Outcomes

| Input Filename | Extracted CASIZ Numbers | Title |
|----------------|-------------------------|-------|
| casiz_12345_and_cas_67890.jpg | [12345, 67890] | casiz_12345_and_cas_67890 |
| 12345_or_67890.png | [12345, 67890] | 12345_or_67890 |
| cas-98765_image.jpg | [98765] | cas-98765_image |
| report_456789.pdf | [456789] | report_456789 |
| randomfile.txt | [] → False | N/A |

---

Would you like me to save it as a `.md` file and provide a download link?