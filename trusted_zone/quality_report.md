# Quality Report: Trusted Zone

## 1. Introduction & Purpose

The Trusted Zone is the cleaned, standardized, and validated data. Data arriving from the Formatted Zone undergoes processes before being stored here, for quality assurance.

The goal of these processes is to make the data consistent, and ensure we can consume the data with confidence, knowing that fundamental structural, format, and content issues have been addressed.

This report details the specific processes implemented for JSON and image data, the rationale behind them, and an analysis of remaining limitations.

---

## 2. Storage Infrastructure

The Trusted Zone is implemented as a new MinIO bucket, `trusted_zone`. To maintain a clear and consistent data lineage, its internal structure mirrors that of the Formatted Zone, with dedicated sub-buckets for different data types:
* `json/`
* `media/`

This separation is established by the `trusted_zone/create.py` script.

---

## 3. JSON Data Quality Processing

Processing is handled by `trusted_zone/process_json.py` and targets data from `json/steam` and `json/steamspy`.

### 3.1. Process: Schema Validation
Each JSON file loaded from the Formatted Zone is validated against a predefined list of required keys (`STEAM_REQUIRED_KEYS` and `STEAMSPY_REQUIRED_KEYS`). The `validate_json_structure` function checks that every record contains all keys from its respective list. This guarantees us schema integrity, so that no records are missing essential fields and prevents `KeyError` exceptions. Files that fail this validation are logged and skipped, preventing incomplete or malformed data from contaminating the Trusted Zone.

### 3.2. Process: Formatting Standardization
JSON files that pass validation are re-serialized using `json.dumps` with `indent=4` and `sort_keys=True` before being saved. This enforces a single, deterministic format for all JSON files.
    * `indent=4` makes the files human-readable for easier debugging and manual inspection.
    * `sort_keys=True` ensures that the order of keys is consistent.

---

## 4. Image Data Quality Processing

Processing is handled by `trusted_zone/process_images.py` and targets data from `media/image/`.

### 4.1. Process: Corruption Check & Channel Standardization
Each file is opened using `Pillow (PIL.Image.open)`, which implicitly checks for file corruption. Immediately after, the image is converted to `'RGB'` mode. This is done because `Image.open` call will raise an error if a file is truncated or corrupted, preventing these files from moving to the Trusted Zone. Converting to `'RGB'` standardizes the channel format, ensuring all images are 3-channel. This fixes inconsistencies like 4-channel `RGBA` (with alpha/transparency) or 1-channel grayscale images, which can break deep learning models that expect a fixed input shape.

### 4.2. Process: Brightness & Contrast Standardization
Histogram Equalization (`ImageOps.equalize`) is applied to every image. This is a generic image enhancement technique that redistributes pixel intensities to improve contrast. It helps to normalize images taken under varying or poor lighting conditions (e.g., images that are too dark or washed out). This provides a more consistent visual baseline for analysis.

### 4.3. Process: Resolution & Aspect Ratio Standardization
All images are standardized to a `(256, 256)` resolution using `ImageOps.pad`. Machine learning models require inputs of a fixed, consistent size. Using `ImageOps.pad` (which adds black bars) rather than resize or crop is a deliberate *generic* choice. It resizes the image to fit within `(256, 256)` while preserving its original aspect ratio and all original content. This prevents distortion (from stretching) or information loss (from cropping), which would be overly specific transformations.

---

## 5. Quality Analysis & Limitations

While the implemented processes establish a great baseline, there are different qualitative aspects that are not yet addressed:

* **JSON Content Validation:** The current process validates the *presence* of keys, not the content of their values. We do not check rules (e.g., `required_age` cannot be negative, `price` must be a valid number, `metacritic_score` must be between 0-100). We also do not validate data types (e.g., `dlc_count` should be an integer).

* **Image Content Quality:** We do not analyze the content of the images. The Trusted Zone may still contain broken link icons, or images that are extremely blurry or watermarked, even if they are valid.

* **Anonymization & Fairness:** The current scripts do not perform any anonymization. The `process_images.py` script does not include logic for detecting and blurring faces or license plates. Similarly, `process_json.py` does not scan text fields (like `detailed_description`) for personally identifiable information (PII) like names or emails. There is also no fairness analysis performed. We do not analyze the data for potential biases (e.g., analyzing screenshots or descriptions for demographic or gender representation).

These limitations represent the next steps in maturing the Trusted Zone. The current implementation successfully guarantees structural and format consistency, but future iterations should focus on JSON content-level validation, image content quality and anonymization/fairness analysis.