from config.config import SNOWFLAKE_CONFIG, REPORT_PATH
from src.snowflake_loader import fetch_active_parks, load_snowflake_data, get_latest_snowflake_date
from src.roller_downloader import download_dashboard
from src.roller_csv_loader import load_roller_csv
from src.revenue_compare import compare_revenue

import os
import time

import pandas as pd


# Use key-based Snowflake connection from config/config.py
print("Using Snowflake connection from config...")
conn_params = SNOWFLAKE_CONFIG

print("Fetching parks from Snowflake...")
parks_list = fetch_active_parks(conn_params)

print("Total parks:", len(parks_list))

print("Opening Roller...")
roller_file = download_dashboard()

# Retry loading CSV in case file is still being written
print("Loading roller CSV...")
for attempt in range(5):
    try:
        roller_df, check_date = load_roller_csv(roller_file)
        break
    except PermissionError:
        print(f"File access denied, retrying ({attempt + 1}/5)...")
        time.sleep(2)

roller_date = pd.to_datetime(check_date).strftime("%Y-%m-%d")

# Get the latest available date in Snowflake
print("Getting latest Snowflake date...")
latest_snowflake_date = get_latest_snowflake_date(conn_params)
print(f"Latest Snowflake date: {latest_snowflake_date}")

# Use the earlier of Roller date or Snowflake date (Snowflake data may be stale)
if latest_snowflake_date and roller_date > latest_snowflake_date:
    print(f"WARNING: Roller date ({roller_date}) is newer than Snowflake data ({latest_snowflake_date})")
    print(f"Using Snowflake date: {latest_snowflake_date}")
    check_date = latest_snowflake_date
else:
    print(f"Using Roller date: {roller_date}")
    check_date = roller_date

print("Fetching Snowflake revenue...")
snowflake_df = load_snowflake_data(
    conn_params,
    check_date,
    parks_list,
)

# If no data returned, try the latest Snowflake date as fallback
if snowflake_df.empty:
    print("WARNING: No data returned! Trying latest Snowflake date...")
    if latest_snowflake_date:
        check_date = latest_snowflake_date
        print(f"Retrying with date: {check_date}")
        snowflake_df = load_snowflake_data(
            conn_params,
            check_date,
            parks_list,
        )

print("Comparing...")
result = compare_revenue(roller_df, snowflake_df)

output_file = os.path.join(REPORT_PATH, "revenue_comparison.xlsx")
match_summary = (
    result["MATCH"]
    .value_counts()
    .reindex(["Match", "Mismatch"], fill_value=0)
    .reset_index()
)
match_summary.columns = ["STATUS", "COUNT"]

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    result.to_excel(writer, sheet_name="Revenue Comparison", index=False)
    match_summary.to_excel(writer, sheet_name="Match Summary", index=False)

print("Report saved at:", output_file)
if os.path.exists(output_file):
    os.startfile(output_file)
