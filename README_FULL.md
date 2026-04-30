# SkyZone Revenue Validation Full Script

This guide is for running the single-file workflow in [sz_rev_valid_full.py](sz_rev_valid_full.py).

Use this script when you want one command to run the full Roller-to-Snowflake revenue validation without writing output into the normal `data/` folders.

## What This Script Does

1. Creates a temporary workspace on your machine.
2. Copies `rsa_key.p8` into that temp workspace for the run.
3. Connects to Snowflake using key-pair authentication.
4. Fetches the active O&O park list.
5. Opens Chrome and logs into Roller.
6. Opens the `Revenue By Park By Day` dashboard.
7. Refreshes and downloads the dashboard export as CSV.
8. Loads the Roller CSV from the temp download folder.
9. Fetches matching revenue from Snowflake.
10. Compares Roller vs Snowflake revenue.
11. Creates the final Excel workbook in the temp output folder.
12. Opens the Excel workbook.
13. Deletes the temp workspace after Excel releases the workbook.

## Files Needed

These files must be in the project root:

```text
sz_rev_valid_full.py
requirements.txt
rsa_key.p8
```

The script has the Roller and Snowflake settings inside the file. If those values change later, update the constants near the top of `sz_rev_valid_full.py`.

## Prerequisites

- Python 3.11 or newer
- Google Chrome installed
- Access to Roller with the configured account
- Snowflake key-pair access
- `rsa_key.p8` available in the project root

This script is a single workflow file, but it still needs Python packages for Snowflake, Selenium, pandas, and Excel writing.

## Step 1: Open PowerShell

Open PowerShell in the project folder:

```powershell
cd C:\Users\wasif.hussain\Desktop\SkyZone_Automation_Wasif
```

## Step 2: Install Dependencies

Run this once, or whenever setting up a new machine:

```powershell
python -m pip install -r requirements.txt
```

## Step 3: Confirm Key File Exists

Make sure this file exists:

```text
rsa_key.p8
```

The script copies this key to a temp folder during the run. It does not move or delete the original key.

## Step 4: Run The Full Script

Run:

```powershell
python sz_rev_valid_full.py
```

## Step 5: Wait For Automation

During the run, the script will:

- connect to Snowflake
- open Chrome
- log into Roller
- download the Roller report
- compare revenue
- create an Excel report

Do not close the Chrome window while Selenium is using it.

## Step 6: Review The Excel Report

When the run completes, Excel opens automatically with:

```text
revenue_comparison.xlsx
```

The workbook contains:

- `Revenue Comparison`
- `Match Summary`

## Temp Cleanup Behavior

The script creates a temp folder with:

```text
parks/
roller_downloads/
output/
key/
```

After Excel opens, the script starts a background cleanup watcher. The temp folder is removed after the workbook is no longer locked by Excel.

If Excel stays open, cleanup waits. If the run fails before Excel opens, the script deletes the temp folder immediately.

## Troubleshooting

If packages are missing, the script prints the missing package names. Install them with:

```powershell
python -m pip install -r requirements.txt
```

If Roller hangs or a page loads too long, rerun:

```powershell
python sz_rev_valid_full.py
```

The script retries the Roller browser workflow up to three times.

If Snowflake key authentication fails, confirm:

- `rsa_key.p8` exists in the project root
- the private key passphrase in `sz_rev_valid_full.py` is correct
- the Snowflake user still has the matching public key configured
