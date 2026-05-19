# SkyZone Revenue Automation - Roller Analytics approach

Automates the daily revenue comparison between Roller and Snowflake.

The pipeline downloads the `Revenue By Park By Day` report from Roller, loads the matching Snowflake revenue for the same date, compares both sources by venue, and writes an Excel report with match/mismatch results.

# Business Purpose

This automation helps validate that revenue loaded into Snowflake matches the source system (Roller).

It is intended to:

- Detect revenue discrepancies quickly
- Reduce manual reconciliation effort
- Provide a daily audit report
- Support finance / data / operations teams


## What It Does

1. Connects to Snowflake.
2. Pulls active O&O parks from `DIMLOCATION`.
3. Opens Roller in Chrome using Selenium.
4. Navigates to the `Revenue By Park By Day` dashboard.
5. Refreshes the dashboard data.
6. Downloads the report as CSV.
7. Loads and standardizes Roller revenue data.
8. Detects the Roller business date.
9. Pulls matching Snowflake revenue.
10. Compares both sources by venue.
11. Generates Excel report.
12. Sends Teams alert when variance exceeds threshold (if enabled).

## Project Structure

```text
SkyZone_QA_Automation_RevAnalytics/
|-- main.py                         # Full pipeline entry point
|-- README.md                       # Instructions for the project in detail
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
|   |-- notification.py             # Notification/Webhook logic

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
- `ROLLER_LOGIN_URL`: Roller login page. Defaults to the production login URL.
- `ROLLER_DASHBOARD_URL`: Roller analytics dashboard page.
- `ROLLER_USERNAME` / `ROLLER_PASSWORD`: Roller credentials used by Selenium.
- `ROLLER_REPORT_NAME`: dashboard tile name to open. Defaults to `Revenue By Park By Day`.
- `ROLLER_RETRIES`: full browser workflow retry count. Defaults to `3`.
- `ROLLER_DOWNLOAD_TIMEOUT`: download wait timeout in seconds. Defaults to `120`.
- `REPORT_PATH`: where the Excel report is written.
- `REPORT_FILE_NAME`: Excel workbook name. Defaults to `revenue_comparison.xlsx`.
- `TEAMS_WEBHOOK_URL`: Teams incoming webhook or workflow URL used for discrepancy alerts.
- `TEAMS_INCLUDE_EXCEL_FILE`: whether to include the workbook as base64 file content in the Teams payload. Defaults to `true`.
- `TOTAL_REVENUE_VARIANCE_THRESHOLD`: dollar tolerance for total revenue variance before alerting. Defaults to `0.01`.
- `ROW_REVENUE_VARIANCE_THRESHOLD`: row-level dollar tolerance used for Match/Mismatch. Defaults to `0.005`.
- `OPEN_REPORT_AFTER_RUN`: whether to open the workbook on this machine after the run. Defaults to `true`.
- `EXCLUDED_ROLLER_VENUES`: comma-separated venue names to remove from the comparison. Defaults to `CLOUDBOUND NEW ROCHELLE NY`.
- `SNOWFLAKE_CONFIG`: Snowflake user, account, warehouse, role, database, schema, private key path, and private key passphrase.

The private key path is resolved relative to the project root when it is not absolute.

Teams alert settings are read from environment variables so secrets do not need to be committed:

```powershell
$env:TEAMS_WEBHOOK_URL = "https://..."
$env:TEAMS_INCLUDE_EXCEL_FILE = "true"
$env:TOTAL_REVENUE_VARIANCE_THRESHOLD = "0.01"
$env:ROW_REVENUE_VARIANCE_THRESHOLD = "0.005"
$env:OPEN_REPORT_AFTER_RUN = "true"
$env:EXCLUDED_ROLLER_VENUES = "CLOUDBOUND NEW ROCHELLE NY"
```

Note: a basic Teams incoming webhook can send the alert text, but it cannot natively upload a local Excel file into the chat. To post the workbook as a file, point `TEAMS_WEBHOOK_URL` at a Teams/Power Automate workflow that reads `reportAttachment.name`, `reportAttachment.contentType`, and `reportAttachment.contentBytes`, creates the `.xlsx` file, and posts it to the chat or channel.

NOTE: Teams Notification (Currently Disabled)
For Git / handover safety, webhook notifications are currently disabled.
TEAMS_WEBHOOK_URL = ""

This means:

- pipeline still runs
- report still generates
- the Excel report opens locally after the run, unless `OPEN_REPORT_AFTER_RUN=false`
- no Teams message is sent

How To Enable Teams Notifications Later

Once audience / channel is confirmed, run PowerShell:

[Environment]::SetEnvironmentVariable(
    "TEAMS_WEBHOOK_URL",
    "enter the url here",
    "User"
)

Then:
Close VS Code
Re-open VS Code
Re-run project

Validate webhook loaded:
python -c "from config.config import TEAMS_WEBHOOK_URL; print(bool(TEAMS_WEBHOOK_URL))"

Expected result:
True

## Running

Run the full automation:

```powershell
python main.py
```

Expected output:

- Roller download file in `data/downloads/`
- Final report at `data/output/revenue_comparison.xlsx`
- Excel workbook opens automatically after the run when `OPEN_REPORT_AFTER_RUN=true`

To stop the workbook from opening locally after a run:

```powershell
$env:OPEN_REPORT_AFTER_RUN = "false"
```

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
- `ROLLER_DOWNLOAD_TIMEOUT = 120`
- `CSV_LOAD_RETRIES = 5`

Each full retry starts a fresh Chrome browser session.

# Roller Date Logic

Currently the Roller dashboard is configured to:
Yesterday

This is the default production run logic.
If running for another date:
Dashboard filter must be manually changed in Roller before executing script.

Examples:
specific day
prior month
month-end validation
custom date range

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
- `src/notification.py`: builds and sends Teams discrepancy alerts when a webhook URL is configured.

## Handover Notes

The pipeline is intentionally parameterized through environment variables first, with compatible defaults in [config/config.py](config/config.py). For handover, prefer changing environment variables instead of editing Python files for credentials, URLs, timeouts, thresholds, excluded venues, and output behavior.

`main.py` is import-safe and exposes `run_pipeline()` for future scheduling or orchestration work. Running `python main.py` still executes the same full process.

## Troubleshooting

If Roller hangs or closes unexpectedly, rerun `python main.py`. The downloader already retries the full browser workflow up to three times.

If Chrome or ChromeDriver fails, update Chrome and reinstall dependencies:

```powershell
python -m pip install --upgrade -r requirements.txt
```

If Snowflake returns no data for the Roller date, `main.py` falls back to the latest Snowflake date when available.

If Snowflake authentication succeeds but table access fails, confirm the configured role is correct. The current production role is `PROD_READER_FR`.

# Expected Terminal Output

Typical successful flow:

Using Snowflake connection from config...
Fetching parks from Snowflake...
Opening Roller...
Downloading report...
Loading roller CSV...
Fetching Snowflake revenue...
Comparing...
Report saved at:
data/output/revenue_comparison.xlsx
