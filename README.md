# SkyZone Revenue Automation

Automates the daily revenue comparison between Roller and Snowflake.

The pipeline downloads the `Revenue By Park By Day` report from Roller, loads the matching Snowflake revenue for the same date, compares both sources by venue, and writes an Excel report with match/mismatch results.

## What It Does

1. Connects to Snowflake.
2. Fetches the active O&O parks from `DIMLOCATION`.
3. Opens Roller in Chrome with Selenium.
4. Logs in and opens the `Revenue By Park By Day` dashboard.
5. Refreshes the dashboard and downloads the report as CSV.
6. Loads and normalizes the Roller CSV.
7. Finds the latest available Snowflake revenue date.
8. Compares Roller revenue against Snowflake revenue.
9. Saves the comparison workbook to `data/output/revenue_comparison.xlsx`.
10. Sends a Teams alert when total revenue variance is above the configured threshold.
11. Includes the Excel workbook in the Teams webhook payload for workflows that create/post the file.

## Project Structure

```text
SkyZone_Automation_Wasif/
|-- main.py                         # Full pipeline entry point
|-- sz_rev_valid_full.py            # Optional single-file temp-workspace runner
|-- README_FULL.md                  # Instructions for sz_rev_valid_full.py
|-- requirements.txt                # Python package requirements
|-- rsa_key.p8                      # Snowflake private key, local secret
|-- config/
|   |-- config.py                   # Paths and Snowflake configuration
|-- src/
|   |-- roller_downloader.py        # Selenium Roller download workflow
|   |-- roller_csv_loader.py        # Extracts and loads Roller CSV data
|   |-- snowflake_client.py         # Snowflake connection helper
|   |-- snowflake_loader.py         # Snowflake query functions
|   |-- revenue_compare.py          # Revenue comparison logic
|-- data/
|   |-- downloads/                  # Generated Roller downloads
|   |-- input/                      # Optional input/reference files
|   |-- output/                     # Generated Excel reports
```

## Requirements

- Python 3.11 or newer
- Google Chrome installed
- Access to Roller with the configured account
- Snowflake key-based authentication using `rsa_key.p8`

Install Python packages:

```powershell
python -m pip install -r requirements.txt
```

## Configuration

Update [config/config.py](config/config.py) before running in a new environment.

Important settings:

- `ROLLER_DOWNLOAD_PATH`: where Roller files are downloaded.
- `REPORT_PATH`: where the Excel report is written.
- `TEAMS_WEBHOOK_URL`: Teams incoming webhook or workflow URL used for discrepancy alerts.
- `TEAMS_INCLUDE_EXCEL_FILE`: whether to include the workbook as base64 file content in the Teams payload. Defaults to `true`.
- `TOTAL_REVENUE_VARIANCE_THRESHOLD`: dollar tolerance for total revenue variance before alerting. Defaults to `0.01`.
- `OPEN_REPORT_AFTER_RUN`: whether to open the workbook on this machine after the run. Defaults to `false`.
- `EXCLUDED_ROLLER_VENUES`: comma-separated venue names to remove from the comparison. Defaults to `CLOUDBOUND NEW ROCHELLE NY`.
- `SNOWFLAKE_CONFIG`: Snowflake user, account, warehouse, role, database, schema, private key path, and private key passphrase.

The private key path is resolved relative to the project root when it is not absolute.

Teams alert settings are read from environment variables so secrets do not need to be committed:

```powershell
$env:TEAMS_WEBHOOK_URL = "https://..."
$env:TEAMS_INCLUDE_EXCEL_FILE = "true"
$env:TOTAL_REVENUE_VARIANCE_THRESHOLD = "0.01"
$env:OPEN_REPORT_AFTER_RUN = "false"
$env:EXCLUDED_ROLLER_VENUES = "CLOUDBOUND NEW ROCHELLE NY"
```

Note: a basic Teams incoming webhook can send the alert text, but it cannot natively upload a local Excel file into the chat. To post the workbook as a file, point `TEAMS_WEBHOOK_URL` at a Teams/Power Automate workflow that reads `reportAttachment.name`, `reportAttachment.contentType`, and `reportAttachment.contentBytes`, creates the `.xlsx` file, and posts it to the chat or channel.

## Running

Run the full automation:

```powershell
python main.py
```

Expected output:

- Roller download file in `data/downloads/`
- Final report at `data/output/revenue_comparison.xlsx`

By default, `main.py` does not open the workbook locally. Set `OPEN_REPORT_AFTER_RUN=true` if you want the old auto-open behavior.

## Roller Download Reliability

[src/roller_downloader.py](src/roller_downloader.py) includes retry handling for common Roller/Selenium failures:

- page load timeout
- long-loading login or dashboard pages
- temporary Selenium browser errors
- missing page elements caused by slow loading

The default behavior is:

- `PAGE_LOAD_TIMEOUT = 60`
- `SCRIPT_TIMEOUT = 30`
- `ROLLER_RETRIES = 3`

Each full retry starts a fresh Chrome browser session.

## Generated Files

The automation creates downloads, extracted CSV files, Excel reports, and Python cache files. These are ignored by `.gitignore` so the repo stays focused on source/config/docs.

Common generated paths:

```text
data/downloads/
data/output/
__pycache__/
src/__pycache__/
config/__pycache__/
```

## Main Modules

- `main.py`: orchestrates the complete process.
- `src/snowflake_client.py`: creates the Snowflake connection.
- `src/snowflake_loader.py`: fetches active parks, latest Snowflake date, and revenue data.
- `src/roller_downloader.py`: downloads the Roller dashboard export.
- `src/roller_csv_loader.py`: extracts ZIP downloads when needed and standardizes Roller columns.
- `src/revenue_compare.py`: merges Roller and Snowflake data and calculates variance.

## Troubleshooting

If Roller hangs or closes unexpectedly, rerun `python main.py`. The downloader already retries the full browser workflow up to three times.

If Chrome or ChromeDriver fails, update Chrome and reinstall dependencies:

```powershell
python -m pip install --upgrade -r requirements.txt
```

If Snowflake returns no data for the Roller date, `main.py` falls back to the latest Snowflake date when available.

If Snowflake authentication succeeds but table access fails, confirm the configured role is correct. The current production role is `PROD_READER_FR`.
