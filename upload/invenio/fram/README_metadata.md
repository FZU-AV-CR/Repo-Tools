# FRAM metadata: where every field comes from

How each metadata field of a FRAM record (model `fram`, schema in `metadata.yaml`) is obtained from the FITS file or calculated from it.

*Last updated: 2026-10-16. Describes the code in `fram_upload.py`, `calibrate.py`, `fram_make_json.py` and `convert_dump.py` as delivered together.*

## Contents

1. [Which tool produces the metadata](#1-which-tool-produces-the-metadata)
2. [Conventions](#2-conventions)
3. [Field reference](#3-field-reference)
4. [Overscan crop, usable area and bias level](#4-overscan-crop-usable-area-and-bias-level)
5. [Night of observation](#5-night-of-observation)
6. [Sky position: centre, radius, footprint, HEALPix](#6-sky-position-centre-radius-footprint-healpix)
7. [FWHM and star count](#7-fwhm-and-star-count)
8. [What each image type gets](#8-what-each-image-type-gets)
9. [Validation and rejection codes](#9-validation-and-rejection-codes)
10. [Header values that are sanitised or deliberately not used](#10-header-values-that-are-sanitised-or-deliberately-not-used)
11. [Differences when converting from the archive dump](#11-differences-when-converting-from-the-archive-dump)
12. [Options that change the metadata](#12-options-that-change-the-metadata)
13. [Known limitations and open items](#13-known-limitations-and-open-items)
14. [Example record](#14-example-record)

---

## 1. Which tool produces the metadata

| Tool | Input | Output |
|---|---|---|
| `fram_upload.py` (adapter for `bulk_async.py` / `async_upload.py`) | FITS files | records created and published directly in the repository |
| `fram_make_json.py` | FITS files | one JSON file per FITS file (mirrored directory tree), to be bulk-imported and paired with the files on S3 |
| `convert_dump.py` | metadata dump of the legacy FRAM archive | the same JSON records, without reading any FITS file |

All three build the record with the **same code**:

- `fram_upload._extract_metadata()` reads the FITS file and returns a flat dictionary of values.
- `fram_upload.header_fields()` sanitises the header-derived fields (shared with `convert_dump.py`).
- `fram_upload.build_invenio_metadata()` turns that dictionary into the record.
- `fram_make_json.build_record()` adds the three bulk-specific parts (related resources, fixed publication date, `files.entries`).
- `calibrate.crop_overscans()` provides the overscan crop (inherited from the legacy archive).

`profile_dump.py` is a read-only profiler of the archive dump. It writes no records.

## 2. Conventions

- **Header.** The header is read with `fits.getheader(path, -1)`, i.e. the last HDU. For tile-compressed files (empty primary HDU) this is the image HDU, for plain files the primary HDU. Both kinds were checked on real files. The FITS file itself is never modified; the original header stays in the file and is shown in the portal UI.
- **Usable area.** The image after the overscan crop (section 4). All image statistics refer to it.
- **Missing values.** A field without a value is **omitted** from the record, never written as `null` or `0.0`. Empty objects are omitted too.
- **Units.** Times are UTC. Angles are degrees. Pixel scale and FWHM are arcseconds. Pixel statistics are ADU. Temperatures are °C. Exposure is seconds.
- **Plausibility ranges.** A numeric header value that is not finite, or lies outside its plausible range, is dropped instead of stored (section 10).

## 3. Field reference

"Header" means a keyword of the FITS header. "Omitted when" says when the field is absent from the record.

### 3.1 Record level

| Key | Value | Notes |
|---|---|---|
| `metadata` | all fields below | |
| `files` | `{"enabled": true}` | `fram_make_json.py` and `convert_dump.py` add `"entries": [{"key": <file name>, "path": <path starting at data3>}]` so a bulk-imported record can be paired with its file on S3 |
| `access` | record `public`, files `public`, embargo inactive | |
| `community` | `fram` | passed to the repository as the community of the record |
| `model` | `fram` | routes the record to the FRAM metadata model |

### 3.2 Descriptive fields (constants)

| Field | Value |
|---|---|
| `resource_type` | `{"id": "c_ddb1"}` (COAR resource type "dataset") |
| `creators` | organisation "FZU Institute of Physics of the Czech Academy of Sciences" |
| `contributors` | organisation "FRAM collaboration", role `ResearchGroup` |
| `publisher` | FZU Institute of Physics of the Czech Academy of Sciences |
| `rights` | `{"id": "4-BY"}` (CC-BY 4.0) |
| `subjects` | Fram, Telescope, Astrophysics, Auger, CTA, Photometric robotic atmosperic monitor, Physics, Paranal, Roque de los Muchachos |
| `additional_descriptions` | one English abstract describing the dataset as FRAM observation data in FITS format |
| `file_types` | `["fits"]` |
| `experiment` | `{"id": "FRAM"}` |

### 3.3 Identification and provenance

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `title` | computed | `FRAM_<site>_<ccd>_<file name without extension>`, e.g. `FRAM_cta-s1_WF0_20251221013923-634-RA` | never |
| `filename` | file path | Path of the file in the archive. Uploader: the part of the path after the `mnt` folder (or relative to the data root if there is no `mnt` folder). Generator and dump converter: the path starting at the anchor folder `data3`, e.g. `data3/cta-s1/2025/20251220/01056/WF0/20251221013923-634-RA.fits` | never |
| `publication_date` | run time | Uploader: day of the run. Generator and converter: `--publication-date`, or `--publication-date-offset-days` days from today, or the day of the run | never |
| `dates` | header | `[{"date": <UTC date of DATE-OBS>, "type": "Created"}]` | never |
| `identifiers` | run time | `[{"identifier": "", "scheme": "url"}]`. Uploader: after the draft is created, `async_upload.py` fills the empty identifier with the record's own landing-page URL. Generator and converter: left empty, because the URL does not exist before the record is created | never (value may be empty) |
| `related_resources` | constant / table | Uploader: two fixed entries, `FRAM_2024_auger` (DOI 10.83100/j3vr-c838) and `FRAM_FZU_root` (DOI 10.83100/ddxy-p647), relation `IsPartOf`. Generator and converter: `FRAM_FZU_root`, preceded by a site-year entry when `--site-year-resources` or `--site-year-resource` supplies a row for the `data3/<site>/<year>/` folders of the path | never |
| `identifier` | not populated | integer placeholder defined in the schema, never filled | always |

### 3.4 Observation

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `type` | header `IMAGETYP` | stripped text, e.g. `object`, `dark`, `flat`, `zero`, `flat1`, `masterdark`, `masterflat`, `bias`, `dcurrent`; `unknown` if the keyword is missing | never |
| `target` | header `TARGET`, `OBJECT` | `<TARGET> / <OBJECT>` when both exist, otherwise whichever exists | calibration frames, or neither keyword present |
| `observation_time` | header `DATE-OBS` | parsed with or without fractional seconds, written as `YYYY-MM-DDTHH:MM:SSZ`. The fraction is **truncated** | never (a frame without a valid `DATE-OBS` is rejected) |
| `observation_night` | computed | local date of the observing night, see [section 5](#5-night-of-observation) | never |
| `exposure` | header `EXPOSURE` | seconds. `EXPTIME` is not used | keyword missing or not numeric |
| `filter` | header `FILTER` | text as stored; `unknown` if the keyword is missing | never |

### 3.5 Instrument and detector

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `site` | file path | The first of `auger2`, `auger`, `cta-n`, `cta-s0`, `cta-s1` that occurs anywhere in the full path (the order matters because `auger` is contained in `auger2`). `fram_make_json.py --site` overrides it. A soft check compares the observatory with header `LONGITUD` and only logs a warning on a mismatch over 2° | never (a frame without a site is rejected) |
| `ccd` | header `CCD_NAME` | text as stored, e.g. `WF0`, `C0`, `NF4` | never (rejected if missing) |
| `camera_serial` | header `PRODUCT_ID` | written as text, e.g. `6204`. `CCD_SER` is **not** used because the same value appears on different cameras | never (rejected if missing) |
| `binning` | header `BINNING` | text such as `1x1`, `2x2` | never (rejected if missing) |
| `image_size.width`, `.height` | header `NAXIS1`, `NAXIS2` | size of the stored image, i.e. the full chip including overscan | never for FITS files (a frame without these keywords is rejected as `E_CROP_FAILED`). Dump converter: when the archive column is empty |
| `image_size.usable_width`, `.usable_height` | computed | shape of the image after the overscan crop ([section 4](#4-overscan-crop-usable-area-and-bias-level)). For master frames the stored size | never (rejected if zero) |
| `instrument` | header `INSTRUME` | stripped text, copied as is | keyword missing or empty |
| `pixel_scale` | computed | arcseconds per stored (possibly binned) pixel. With a solved WCS (science frames): the square root of the absolute determinant of the WCS pixel-scale matrix, times 3600. Otherwise from the nominal telescope scale: the absolute value of header `CDELT1T` times 3600 | no WCS and no `CDELT1T` |

### 3.6 Pointing and sky position

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `alt_az.altitude` | header `TEL_ALT` | degrees, no range check | keyword missing |
| `alt_az.azimuth` | header `TEL_AZ` | degrees, no range check | keyword missing |
| `center.ra`, `center.dec` | computed | sky coordinates of the centre pixel of the usable area, from the WCS in the header (RA 0 to 360) | no sky position, see [section 6](#6-sky-position-centre-radius-footprint-healpix) |
| `radius` | computed | half of the great-circle distance between the two opposite corner pixels, in degrees | no sky position |
| `center_geo.lat`, `.lon` | computed | latitude = Dec, longitude = RA wrapped to −180..180 (`RA − 360` when RA > 180). Exists for the geo-point index | no sky position |
| `footprint` | computed | GeoJSON outline of the image corners with RA wrapped to −180..180 | no sky position, or the corner computation failed |
| `healpix_idx` | computed | HEALPix pixel of the centre, NSIDE = 64, RING ordering, from `theta = 90° − Dec` and `phi = RA` (range 0 to 49151) | no sky position |

### 3.7 Image statistics and quality

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `mean` | computed | mean of the usable area after the overscan crop, **bias not subtracted**, in ADU. Master frames: the image as stored | not finite |
| `median` | computed | median of the same area and definition | not finite |
| `robust_std` | computed | 1.4826 × median absolute deviation, calculated on every 7th pixel of the usable area, in ADU. Not influenced by stars | not finite |
| `fwhm_arcsec` | computed | median FWHM of clean stars in arcseconds, see [section 7](#7-fwhm-and-star-count) | not a science frame, `sep` not installed, `--no-quality`, or fewer than 20 clean stars |
| `fwhm_pixels` | computed | the same measurement in pixels | same as `fwhm_arcsec` |

### 3.8 Camera state

| Field | Source | How it is obtained or calculated | Omitted when |
|---|---|---|---|
| `ccd_temperature` | header `CCD_TEMP` | chip temperature in °C. Values outside −150..80 are dropped | missing or implausible |
| `air_temperature` | header `CCD_AIR` | air temperature inside the camera at the detector in °C (a different quantity from the chip temperature). Values outside −60..100 are dropped | missing or implausible |
| `bias_level` | header `BIASAVG` | bias level in ADU, **measured from the overscan strip** by `calibrate.crop_overscans()` ([section 4](#4-overscan-crop-usable-area-and-bias-level)). Never estimated from a temperature model | no overscan measured, and always for master frames |

## 4. Overscan crop, usable area and bias level

Master frames (`masterdark`, `masterflat`, `bias`, `dcurrent`) are already processed products. They are **not** cropped and no bias is measured. All their statistics describe the image as stored.

For every other frame `calibrate.crop_overscans(image, header, subtract=False)` is applied. The bias is **never subtracted** in the metadata, so `mean` and `median` equal the legacy archive values (checked on one real frame: 771.5434 and 748, identical). The crop works like this:

1. If the header has a `DATASEC` keyword, the image is cut to that section and `CRPIX1`/`CRPIX2` are shifted accordingly. The original keyword is kept as `DATASEC0` in the working copy.
2. Otherwise a table of known raw shapes is used:

| Raw shape (rows × columns) | Binning | Overscan strip used for the bias | Crop | Result |
|---|---|---|---|---|
| 4124 × 4148 | any | rows −14:−4, columns 800:−800 | rows 11:−17, columns 33:−19 | 4096 × 4096 |
| 4127 × 4144 | any | rows −14:−4, columns 800:−800 | rows 11:−20, columns 30:−18 | 4096 × 4096 |
| 2062 × 2074 | 2x2 | rows −7:−2, columns 400:−400 | rows 5:−9, columns 17:−9 | 2048 × 2048 |
| 2063 × 2072 | 2x2 | rows −7:−2, columns 400:−400 | rows 5:−10, columns 15:−9 | 2048 × 2048 |
| 1026 × 1062 | any | last 5 columns | rows 2:, columns 0:1056 | 1024 × 1056 |

3. The bias level is the **3σ-clipped mean** of the overscan strip. It is written to the working header as `BIASAVG` and becomes `bias_level`. A frame whose shape is not in the table gets no `bias_level`.
4. A special case flips the image and the WCS keywords vertically for MICCD frames (serial ≥ 6000, header `SHIFT`, exactly 4096 × 4096).

The crop and the bias regions are inherited unchanged from the legacy archive code. The linearisation tables in `calibrate.py` are **not** used for the metadata. They affect only the image preview in the portal.

## 5. Night of observation

`observation_night` is the local calendar date of the night in which the frame was taken, in `YYYYMMDD` form. It is calculated so that all frames of one night get the same date even when the exposure is after UTC midnight:

```
night = date( DATE-OBS + longitude * 240 s − 12 h )
```

Here `longitude * 240 s` is the longitude in degrees east converted to a time shift (24 h per 360°).

- The longitude is the header keyword `LONGITUD`.
- If the keyword is missing, a per-site fallback is used: `auger` and `auger2` −69.4497, `cta-n` −17.89, `cta-s0` and `cta-s1` −70.32482, any other site 0. The value for `auger2` is an assumption (the same as `auger`).

Example: `DATE-OBS` 2025-12-21T01:39:23, longitude −70.3769 gives night `20251220`.

## 6. Sky position: centre, radius, footprint, HEALPix

A frame gets a sky position **only if** all of these hold: `type` is `object`, the header has `CTYPE1`, the WCS can be built, and the calculated centre is finite with |Dec| ≤ 90°. Otherwise the five position fields (`center`, `radius`, `center_geo`, `footprint`, `healpix_idx`) are all omitted. Calibration frames never have a position. Position fields are never `0.0` placeholders.

The WCS is taken from the header **after** the overscan crop (the crop shifts `CRPIX`). With `w` and `h` the usable width and height:

- **Centre:** the world coordinates of pixel (`w/2`, `h/2`).
- **Radius:** half of the great-circle distance between pixels (0, 0) and (`w`, `h`).
- **Footprint:** the world coordinates of the pixels (0,0), (`w`,0), (`w`,`h`), (0,`h`) and back to (0,0), RA wrapped to −180..180:
  - normal case: GeoJSON `Polygon`;
  - the polygon crosses the ±180° seam (longitude spread over 180°): `Polygon` with `"orientation": "right"`;
  - a celestial pole lies inside the field (the centre is within `radius` of the pole): a conservative `envelope` over the full longitude range, from the field's near edge to the pole, because a four-corner polygon is invalid there.
- **HEALPix index:** `healpy.ang2pix(64, radians(90° − Dec), radians(RA))`. RING ordering is the default and was reproduced against an already uploaded record (index 46875).

## 7. FWHM and star count

Needs the optional package `sep` (SExtractor core). Without it, or with `--no-quality`, the FWHM fields are left out and nothing else changes. It is measured only on science frames (`type` = `object`).

1. The image is cut to the central window of `--quality-size` pixels per side (default 2048; 0 = whole frame). Smaller frames are used entirely.
2. The background is estimated and subtracted, stars are detected at 5σ above the background (at least 5 connected pixels).
3. A star counts as **clean** if it has no extraction flag, is round (axis ratio over 0.7), is wider than 0.5 px, and its peak is below 40 000 ADU (excludes saturated stars).
4. The number of clean stars is the star count. It is **not stored** in the record. It only decides whether the FWHM is reported.
5. With at least **20** clean stars, `fwhm_pixels` = median of `2.3548 × sqrt((a² + b²) / 2)` over the clean stars, where `a` and `b` are the second-moment widths. `fwhm_arcsec` = `fwhm_pixels × pixel_scale`.

Treat FWHM as a **relative indicator** of image quality, not an absolute measurement. FRAM stars are only about 1.5 to 2 pixels wide (undersampled). Measured on real frames, the central 2048 window agrees with the whole frame within about 5 %, but the centre is usually the best-focused part of the field, so it reads slightly optimistic.

## 8. What each image type gets

| Fields | Science (`object`) with WCS | Science without WCS | Raw calibration (`dark`, `flat`, `zero`, `flat1`) | Master (`masterdark`, `masterflat`, `bias`, `dcurrent`) |
|---|---|---|---|---|
| Descriptive, `title`, `filename`, `dates`, `type`, `observation_time`, `observation_night`, `exposure`, `filter` | yes | yes | yes | yes |
| `target` | yes | if `TARGET` or `OBJECT` exists | no | no |
| `site`, `ccd`, `camera_serial`, `binning`, `image_size` | yes | yes | yes | yes (usable size = stored size) |
| `instrument`, `alt_az`, `ccd_temperature`, `air_temperature` | if in header | if in header | if in header | if in header |
| `center`, `radius`, `center_geo`, `footprint`, `healpix_idx` | yes | no | no | no |
| `pixel_scale` | from WCS | from `CDELT1T` | from `CDELT1T` | from `CDELT1T` |
| `mean`, `median`, `robust_std` | after crop | after crop | after crop | as stored |
| `bias_level` | if overscan measured | if overscan measured | if overscan measured | never |
| `fwhm_arcsec`, `fwhm_pixels` | yes | yes (if enough stars) | no | no |

## 9. Validation and rejection codes

A frame that fails a check gets **no record**. The uploader writes the reason into the `error` column of `upload_stats.csv` with status `skipped_invalid`. The generator writes it to `errors.csv`. The dump converter writes it to `conversion_stats.csv`.

| Code | Meaning |
|---|---|
| `E_UNREADABLE` | the file is corrupt or not FITS (no OS error number) |
| `E_BAD_DATE` | `DATE-OBS` is missing or has an unknown format |
| `E_CROP_FAILED` | the overscan crop raised an error (e.g. a keyword needed for the crop is missing) |
| `E_EMPTY_OR_CONSTANT_IMAGE` | the image is empty or all pixels have the same value |
| `E_MISSING_FIELD: <field>` | one of `site`, `ccd`, `camera_serial`, `binning` has no value |
| `E_NO_USABLE_SIZE` | usable width or height is zero or missing |
| `E_BAD_DATE_RANGE` | observation year is before 2005 or later than next year |
| `E_BAD_EXPOSURE` | negative exposure |

**Not rejected:** real I/O errors (an OS error number, e.g. input/output error, timeout) and out-of-memory errors. These are raised and recorded as `failed`, so the engine retries them, also on the next run. A rejected frame (`skipped_invalid`) is final and is not retried.

The checks do **not** judge scientific quality. Cloudy or defocused frames are kept. Their star count and FWHM show the problem.

## 10. Header values that are sanitised or deliberately not used

**Sanitised before use.** A value that is not finite or lies outside its range is dropped: `CCD_TEMP` −150..80 °C, `CCD_AIR` −60..100 °C.

**Not used because they proved unreliable in real headers:**

| Keyword | Problem seen |
|---|---|
| `GAIN` | La Palma G2 reports milli-units (1167 instead of 1.167) |
| `TEMPPWR` | G2 reports a raw number (39426, labelled as percent) and G4 a fraction 0 to 1 |
| `MOONDIST` | negative nonsense (−9e-36) in some headers |
| `AMBTEMP`, `AMBHUMIDITY`, `WAVELENGTH` | identical constants in every header seen, not measurements |
| `CCD_SER`, `CCD_TYPE`, `CCD_CHIP` | the same serial appears on different cameras. A master dark from a G2 camera carries G4 values |
| `FOC_TEMP` | nonsense values (573) |
| `EXPTIME` | can differ from `EXPOSURE` (a master dark had 19 s vs 180 s). `EXPOSURE` matches the file name |
| weather keywords | named `PARANAL.*` or `WEATHER.*` depending on the site, and absent in recent Auger headers |

These stay in the FITS header, which the portal shows unchanged.

## 11. Differences when converting from the archive dump

`convert_dump.py` produces the same record structure by building the same internal dictionary from a dump row and passing it to the same builder. Where a value comes from differs:

| Item | FITS tools | `convert_dump.py` |
|---|---|---|
| `mean`, `median` | computed from pixels | archive columns (cropped, bias not subtracted: the same definition, verified on one frame) |
| `robust_std`, `fwhm_arcsec`, `fwhm_pixels` | computed | **not available** (need pixels), so absent |
| `observation_night`, `site`, `ccd`, `camera_serial`, `binning`, `exposure`, usable size | header and path | archive columns |
| `filter` | header text | archive column. `UNK`, empty or missing become `unknown` |
| `center`, `radius` | from the WCS | archive columns `ra`, `dec`, `radius`. `center_geo` and `healpix_idx` are recomputed from them |
| `footprint` | from the WCS corners | archive polygon text converted to GeoJSON (same polygon, but the ring may start at another corner and run in another direction) |
| `pixel_scale` | WCS matrix | CD matrix from the stored keywords by the same formula, otherwise `CDELT1T` |
| `instrument`, temperatures, `bias_level`, `alt_az` | header | the archive's stored (already cropped) header, with the same sanitising |

Rows are skipped when the path is not under `data3/`, when a required field is missing, and for duplicates. Paths starting with `calibrations/` get the prefix `data3/`.

## 12. Options that change the metadata

| Option | Tool | Effect |
|---|---|---|
| `--quality-size N` | uploader, generator | window for the FWHM measurement (default 2048, 0 = whole frame). **Not recorded in the metadata**, so use one value for the whole run |
| `--no-quality` | uploader, generator | no FWHM fields |
| `--publication-date`, `--publication-date-offset-days` | generator, converter | `publication_date` of all records |
| `--site-year-resources`, `--site-year-resource`, `--require-site-year-resource` | generator, converter | adds the site-year entry to `related_resources`, optionally rejects files without one |
| `--site` | generator | forces `site` for every file |
| `--anchor` | generator, converter | folder name where `filename` starts (default `data3`) |
| `--exclude-dirs`, `--date-pattern` | uploader, generator | choose which day folders and directories are processed. No effect on values |
| `--include-paths`, `--skip-paths` | generator | choose which subtrees are processed. No effect on values |
| `--extract-concurrency` (uploader), `--workers` (generator) | uploader, generator | memory and speed only. No effect on values |

## 13. Known limitations and open items

1. **FWHM is approximate** (section 7) and depends on `--quality-size`, which is not stored.
2. **`bias_level` is empty** for masters, for cameras whose raw shape is not in the crop table, and for headers without overscan.
3. **`instrument` is inconsistent** between sites (`WF0`, `BART 254/1600 mm`, `FRAM`, `CTA-S1 FRAM`, `WF 4kx4k CCD`) because the observatory software fills `INSTRUME` differently. `pixel_scale` identifies the optics more reliably.
4. **`observation_time` is truncated to seconds**, and `target` is the combined text. Both match the records already uploaded.
5. **The uploader writes the same two `related_resources` for every record**, including `FRAM_2024_auger` for non-Auger sites. The generator uses the site-year table instead.
6. **Typos in the constant texts** ("atmosperic" in `subjects`, "Phototometric" in the abstract) are kept so that new records match those already uploaded.
7. **`identifiers`** holds only a URL (or an empty value). It is not a persistent identifier such as a DOI. Whether the repository assigns a DOI to each record is not decided by this code.
8. **Records uploaded earlier** do not carry the new fields (`mean`, `median`, `robust_std`, `instrument`, `pixel_scale`, `ccd_temperature`, `air_temperature`, `bias_level`, `fwhm_*`) and may still contain `0.0` placeholders for calibration frames. They become consistent only after a new upload.
9. **Night of observation** at `auger2` without `LONGITUD` uses an assumed longitude. The site-versus-longitude check only warns.
10. **Verification status.** The code was tested on nine real files (G2 and G4 cameras, compressed and plain, 1×1 and 2×2, science, raw dark, master dark, master flat) and compared with one archive row, which matched. **Not yet tested:** the CMOS camera (serial 80011), fields containing a pole or crossing RA 0°, science frames without a WCS in real data, master frames in the archive dump, and a full run over the whole archive.

## 14. Example record

Science frame `20251221013923-634-RA.fits` (cta-s1, WF0), produced by `fram_make_json.py`. Constant descriptive fields are shortened.

```json
{
  "metadata": {
    "resource_type": {
      "id": "c_ddb1"
    },
    "title": "FRAM_cta-s1_WF0_20251221013923-634-RA",
    "publication_date": "2026-10-13",
    "identifiers": [
      {
        "identifier": "",
        "scheme": "url"
      }
    ],
    "related_resources": [
      {
        "title": "FRAM_FZU_root",
        "...": "..."
      }
    ],
    "dates": [
      {
        "date": "2025-12-21",
        "type": {
          "id": "Created"
        }
      }
    ],
    "experiment": {
      "id": "FRAM"
    },
    "(creators, contributors, publisher, subjects, rights, additional_descriptions, file_types)": "...",
    "target": "1056 / C/2025 R2 (SWAN)",
    "type": "object",
    "observation_time": "2025-12-21T01:39:23Z",
    "observation_night": "20251220",
    "exposure": 120.0,
    "site": "cta-s1",
    "ccd": "WF0",
    "camera_serial": "6204",
    "filter": "V",
    "binning": "1x1",
    "image_size": {
      "width": 4144,
      "height": 4127,
      "usable_width": 4096,
      "usable_height": 4096
    },
    "alt_az": {
      "altitude": 42.0441346237842,
      "azimuth": 133.693488572378
    },
    "filename": "data3/cta-s1/2025/20251220/01056/WF0/20251221013923-634-RA.fits",
    "mean": 771.543439745903,
    "median": 748.0,
    "robust_std": 69.6822,
    "instrument": "CTA-S1 FRAM",
    "pixel_scale": 14.149957799230636,
    "ccd_temperature": -19.96206,
    "air_temperature": 23.99704,
    "bias_level": 550.555661199588,
    "fwhm_arcsec": 30.667922475076846,
    "fwhm_pixels": 2.1673508084062503,
    "center": {
      "ra": 9.826679535626191,
      "dec": 9.778273598285429
    },
    "radius": 11.181284161244378,
    "footprint": {
      "type": "Polygon",
      "coordinates": [
        [
          [
            17.7288,
            1.8258
          ],
          [
            1.9691,
            1.8005
          ],
          [
            1.5368,
            17.5458
          ],
          [
            18.0758,
            17.582
          ],
          [
            17.7288,
            1.8258
          ]
        ]
      ]
    },
    "center_geo": {
      "lat": 9.778273598285429,
      "lon": 9.826679535626191
    },
    "healpix_idx": 20103
  },
  "files": {
    "enabled": true,
    "entries": [
      {
        "key": "20251221013923-634-RA.fits",
        "path": "data3/cta-s1/2025/20251220/01056/WF0/20251221013923-634-RA.fits"
      }
    ]
  },
  "access": {
    "record": "public",
    "files": "public",
    "embargo": {
      "active": false,
      "reason": null
    }
  },
  "community": "fram",
  "model": "fram"
}
```

The matching master flat record (`sample_record_masterflat.json`) has no `target`, no sky position, no `bias_level` and no FWHM.
