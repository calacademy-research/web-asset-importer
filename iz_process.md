### **Detailed Business Process Overview: Ingesting and Managing Images for the Invertebrate Zoology Collection**

The **Invertebrate Zoology (IZ) Image Importer** is an automated system designed to scan, process, and manage specimen images for the **Specify** database. It follows a structured approach to ingest images, extract metadata, assign them to catalog records, and ensure compliance with copyright and licensing policies.

---

## **Step 1: Scanning and Identifying New Images**
The importer operates **only on a single production directory**:  
📂 **`/letter_drives/n_drive/izg/IZ Images_Curated for CAS Sci Computing ingest`**

Within this directory, the system **recursively scans subdirectories** for image files matching **approved extensions**:
- `.jpg`, `.jpeg`, `.tiff`, `.tif`, `.png`, `.dng`

**Files that are ignored:**
- Hidden system files (e.g., `.DS_Store`)
- Non-image files

If a `key.csv` file is present, it **applies metadata settings to all subdirectories** **until another `key.csv` is encountered**, allowing:
- **Global metadata** at higher directory levels
- **Overrides** for specific subdirectories

🔹 **Example Structure:**
```
/IZ Images_Curated for CAS Sci Computing ingest/
    ├── key.csv  (applies to everything inside)
    ├── Specimen Batch 1/
    │   ├── cp-0455 sigalionidae lavender/
    │   │   ├── cp_0455_img_01.jpg
    │   │   ├── cp_0455_img_02.jpg
    │   │   ├── key.csv  (overrides for this subdirectory)
    │   ├── cp-0449 polyplacophora cryptoplax/
    │   │   ├── vip2015__2015_04_16_4230.jpg
    │   │   ├── vip2015__2015_04_16_4231.jpg
```
Here, the `key.csv` in `/Specimen Batch 1/` applies to all subdirectories **unless another `key.csv` is present** inside `cp-0455 sigalionidae lavender/`.

---

## **Step 2: Mapping Metadata to EXIF Fields**
During processing, **metadata from `key.csv` and Specify constants** is mapped to EXIF fields. This ensures that the correct attribution, copyright, and cataloging details are preserved.

### **EXIF Mapping Table**
| **EXIF Field**                         | **Specify attachment record  field** |
|-----------------------------------------|--------------------------------------|
| `EXIF:Artist`                           | `MetadataText`                       |
| `EXIF:CreateDate`                       | `DateImaged`                         |
| `EXIF:ImageDescription`                 | `Title`                              |
| `IPTC:CopyrightNotice`                  | `CopyrightHolder`                    |
| `IPTC:By-line`                          | `MetadataText`                       |
| `IPTC:Caption-Abstract`                 | `Title`                              |
| `XMP:Credit`                            | `Credit`                             |
| `XMP:Creator`                           | `MetadataText`                       |
| `XMP:Usage`                             | `License`                            |
| `XMP:UsageTerms`                        | `License`                            |
| `XMP:CreateDate`                        | `FileCreatedDate`                    |
| `XMP:Title`                             | `Title`                              |
| `XMP:DateCreated`                       | `DateImaged`                         |
| `EXIF:Copyright`                        | `CopyrightHolder`                    |
| `XMP:Rights`                            | `CopyrightHolder`                    |
| `IFD0:Copyright`                        | `CopyrightHolder`                    |
| `XMP:RightsUsageTerms`                  | `License`                            |
| `XMP:PlusImageSupplierName`             | `Credit`                             |
| `Photoshop:Credit`                      | `Credit`                             |


---

## **Step 3: Assigning CASIZ Numbers**
CASIZ is stored in specify's CollectionObject record as "CatalogNumber" and is the key field for all 
specimen occurrances. It is extracted using the hierustic below to link image files to the existing 
CollectionObject record. If no CollecitonObject with that catalog number exists,  no import is 
done.
### **1️⃣ Filename CASIZ (Highest Priority)**
If the CASIZ number appears **in the filename**, it takes precedence over all other sources.

**Valid Filename Examples:**
- `casiz_214769.jpg` → **CASIZ: 214769**
- `CASIZ-83627_Dorsal.png` → **CASIZ: 83627**
- `specimen CASIZ 198356.tif` → **CASIZ: 198356**

If a filename contains multiple CASIZ numbers separated by 'and' or 'or', the system assigns all matched 
numbers.

---

### **2️⃣ Metadata (EXIF) CASIZ**
If no CASIZ number is found in the filename, the system extracts metadata from EXIF fields.


If a **CASIZ number is found**, it is assigned to the image.

### **3️⃣ Directory Name CASIZ**
If the filename and metadata do not contain a CASIZ number, the system checks the directory structure.

🔹 Example Valid Directory Paths:

/IZ Images_Curated for CAS Sci Computing ingest/specimen_batch/casiz 198356/ → CASIZ: 198356
/Specimen_Labels/CASIZ 987654 labeled/ → CASIZ: 987654
This method is only used if no CASIZ number is found in filenames or metadata.
---

## **Step 4: Handling the "Remove" Flag**
The `remove` field in `key.csv` provides a way to **clean up bad data** and **remove incorrect imports**.

### **How Removal Works:**
1. **Scanning for "remove"**
2. **Lookup and Deletion**
3. **Safe Reprocessing**

### **Practical Example:**
To remove a batch of images, add a `key.csv` with:
```
remove=true
```

---

## **Final Summary**
✔ **Scans only `/letter_drives/n_drive/izg/IZ Images_Curated for CAS Sci Computing ingest/`**  
✔ **Uses `key.csv` for metadata, applying settings to subdirectories**  
✔ **Assigns CASIZ numbers in priority order: Filename > Metadata > Directory Name**  
✔ **Ensures metadata is correctly mapped to EXIF fields**  
✔ **Allows curators to remove and reprocess images via `key.csv`**  

This system ensures **structured, automated, and accurate** specimen image ingestion. 🚀


