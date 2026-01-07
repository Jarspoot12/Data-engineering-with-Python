# Data Directory

This directory follows a standard data engineering layout.

## Structure
- `raw/`  
  Original, immutable datasets as obtained from the source.

- `interim/`  
  Intermediate datasets produced during cleaning or transformation steps. Files in this directory represent partial processing stages and are not intended for final analysis or consumption.

- `processed/`  
  Final, clean datasets ready for analysis or loading into a database.

## Large Datasets Policy
Large datasets are **not** stored in this repository.

A dataset is considered "large" if:
- Its size exceeds ~30–50 MB, or
- It can be reproducibly obtained from a public source, or
- It is subject to licensing or usage restrictions.

Instead of committing large files, this repository provides:
- Download links
- Source descriptions
- Scripts to retrieve or regenerate the data

## External Data Sources

| Dataset | Source | Access |
|-------|------|------|
| Sample users dataset | Kaggle | https://www.kaggle.com/... | --- example ---
| Internal sample data | Synthetic | Generated via scripts |

## Notes
Small sample files may be included for demonstration purposes only.
