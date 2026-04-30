# SkyZone Revenue Automation

Automated pipeline that fetches revenue data from Snowflake and Roller, then compares and generates reports.

## Overview

This project automates the end-to-end process of:
1. Fetching active park names from Snowflake
2. Downloading revenue data from Roller dashboard
3. Comparing Roller vs Snowflake revenue
4. Generating an Excel comparison report

## Project Structure

```
SkyZone_Automation_Wasif/
├── main.py                 # Entry point - runs the full pipeline
├── requirements.txt       # Python dependencies
├── config/
│   └── config.py          # Configuration (paths, credentials)
├── src/
│   ├── snowflake_loader.py    # Fetches parks & revenue from Snowflake
│   ├── snowflake_client.py    # Snowflake connection handler
│   ├── roller_downloader.py   # Automates Roller web scraping
│   ├── roller_csv_loader.py   # Processes downloaded Roller CSV
│   └── revenue_compare.py     # Compares and merges data
└── data/
    ├── downloads/         # Downloaded Roller CSV files
    ├── input/            # Input files (parks list)
    └── output/           # Generated reports
```

## Flow Diagram

```
┌─────────────────┐
│   main.py       │
│ (Entry Point)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────────┐
│ Snowflake       │────▶│ Fetch Active     │
│ Loader          │     │ Parks List       │
└─────────────────┘     └──────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │ Roller Downloader      │
                    │ (Selenium Automation)  │
                    │ 1. Login to Roller     │
                    │ 2. Navigate Dashboard │
                    │ 3. Click Refresh       │
                    │ 4. Open Download      │
                    │ 5. Select CSV Format  │
                    │ 6. Download File      │
                    └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │ Roller CSV Loader     │
                    │ (Process & Validate)  │
                    └────────────────────────┘
                                 │
                                 ▼
┌─────────────────┐     ┌────────────────────────┐
│ Snowflake       │◀───▶│ Revenue Compare        │
│ Revenue Data    │     │ (Merge & Compare)      │
└─────────────────┘     └────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │ Excel Report           │
                    │ (Output)               │
                    └────────────────────────┘
```

## Prerequisites

- Python 3.11+
- Chrome browser installed
- Snowflake credentials configured in `config/config.py`
- Roller credentials (dataopsedw2@skyzone.com)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

Run the full pipeline:

```bash
python main.py
```

The script will:
1. Connect to Snowflake and fetch active parks
2. Open Chrome and automate Roller login
3. Navigate to Revenue By Park By Day dashboard
4. Click refresh to update data
5. Open download dialog and select CSV format
6. Download the revenue data
7. Compare Roller data with Snowflake data
8. Generate a comparison Excel report

## Output

- Downloaded CSV: `data/downloads/roller_data.csv`
- Comparison Report: `data/output/revenue_comparison.xlsx`

## Configuration

Edit `config/config.py` to modify:
- Snowflake connection parameters
- Roller credentials
- Download paths
- Report output path

## Dependencies

- `pandas` - Data manipulation
- `selenium` - Web automation
- `webdriver-manager` - Chrome driver management
- `snowflake-connector-python` - Snowflake connection
- `openpyxl` - Excel file handling