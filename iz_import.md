# `key.csv` End‑User Reference

`key.csv` lets you control how each image is imported without touching code.  Place the file in the same folder as your images (or any parent folder); the importer walks **upwards** until it finds the first `key.csv`.

* Two‑column CSV: **key**, **value**
* Keys are case‑insensitive; blank values are ignored.
* Unknown keys are ignored.

---

## Supported Keys

| Key (column 0)                     | Expected Value                                                     | What It Does                                                                                                                                                                     |
| ---------------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CopyrightDate`                    | Date in **`YYYY‑MM‑DD`**, **`MM/DD/YYYY`**, or **`Month D, YYYY`** | Sets the year shown in Specify’s *Copyright Date* field. If omitted the importer falls back to the imageʼs EXIF CreateDate.                                                      |
| `CopyrightHolder`                  | Text                                                               | Sets the *Copyright Holder* for EXIF embedding and for Specify.                                                                                                                  |
| `Credit`                           | Text                                                               | Written to the EXIF/IPTC *Credit* tag and to Specifyʼs *Credit* field.                                                                                                           |
| `License`                          | Text                                                               | Saved to Specifyʼs *License* field and XMP Usage tags.                                                                                                                           |
| `Remarks`                          | Text                                                               | Copied into Specifyʼs *Remarks*.                                                                                                                                                 |
| `IsPublic`                         | `true` / `false`                                                   | `true` → image is *public* (no redaction); `false` → image is flagged *not public* and attachment is created with redaction enforced.                                            |
| `subType`                          | Text                                                               | Fills Specifyʼs *Subtype* (e.g. `dorsal`, `ventral`).                                                                                                                            |
| `createdByAgent`                   | Agent name                                                         | Looked up (fuzzy match) in Specify and linked as *Created By*.  Overrides the default `AGENT_ID`.                                                                                |
| `creator` *(alias `MetaDataText`)* | Text                                                               | Goes into EXIF *Artist* / IPTC *By‑line* and Specifyʼs *Metadata Text*.  Also used for agent lookup when `createdByAgent` is absent.                                             |
| `remove`                           | `true` / `false`                                                   | When **`true`** the file is **deleted** from the image DB and detached from Specify if already present; the importer logs the action and skips further processing for that file. |
| `erase_exif_fields`                | `true` / `false`                                                   | When **`true`** the importer **blanks** the following tags before continuing:<br>• `XMP:Title`<br>• `IPTC:Caption‑Abstract`<br>• `EXIF:ImageDescription`                         |

---

### Minimal Example

```csv
CopyrightHolder,California Academy of Sciences
IsPublic,false
subType,dorsal
creator,Jane Doe
```

### Full Example (demonstrating `remove` & `erase_exif_fields`)

```csv
remove,true
erase_exif_fields,true
```

Above file would cause every image in the directory to be removed from the database **and** have the specified EXIF fields cleared.

---

**Tip:** Keep one `key.csv` per logical batch.  Place it at the highest level whose settings you want to inherit; subfolders without their own `key.csv` will use the nearest one up the tree.
