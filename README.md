# lris_data_extraction
Code for automated extraction and structuring of Jamabandi land records into LRIS ready datasets, supporting scalable Farmer and Farm registry creation for AgriStack in Jammu &amp; Kashmir.
This repository contains a Python-based prototype developed to demonstrate
automated extraction and structuring of Jamabandi land record data as per
LRIS data extraction requirements.

## What the code does
- Parses cultivator details into structured fields
- Splits multiple Khasra numbers into individual land-parcel records
- Generates structured CSV output for ingestion into LRIS
- Demonstrates automation-first, human-verification-assisted workflow

## Files
- `lris_data_extraction.py` – Main data extraction script
- `sample_input/` – Sample input CSV used for demonstration

## Assumptions
- The input file is a valid CSV file and represents structured output generated from scanned Jamabandi records.
- Column headers in the input dataset are stored as string values (for example "1", "2", "5", "7"), and these consistently correspond to Jamabandi fields such as Khewat, Khata, cultivator details, and Khasra numbers.
- Values in Column 7 (Khasra numbers) are strings or can be safely converted to strings for parsing and splitting.
- All required Python libraries (pandas, re, random, string) are correctly installed and imported, with no missing dependencies.
