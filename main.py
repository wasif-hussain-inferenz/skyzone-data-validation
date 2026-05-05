from config.config import (
    EXCLUDED_ROLLER_VENUES,
    OPEN_REPORT_AFTER_RUN,
    REPORT_PATH,
    SNOWFLAKE_CONFIG,
    TEAMS_INCLUDE_EXCEL_FILE,
    TEAMS_WEBHOOK_URL,
    TOTAL_REVENUE_VARIANCE_THRESHOLD,
)
from src.snowflake_loader import fetch_active_parks, load_snowflake_data, get_latest_snowflake_date
from src.roller_downloader import download_dashboard
from src.roller_csv_loader import load_roller_csv
from src.revenue_compare import compare_revenue
from src.notification import notify_if_revenue_discrepancy

import os
import time

import pandas as pd


# Use key-based Snowflake connection from config/config.py
print("Using Snowflake connection from config...")
conn_params = SNOWFLAKE_CONFIG

print("Fetching parks from Snowflake...")
parks_list = fetch_active_parks(conn_params)
excluded_venues = set(EXCLUDED_ROLLER_VENUES)
if excluded_venues:
    original_park_count = len(parks_list)
    parks_list = [
        park
        for park in parks_list
        if " ".join(str(park).upper().split()) not in excluded_venues
    ]
    excluded_count = original_park_count - len(parks_list)
    print(f"Excluded parks from comparison: {excluded_count}")
    print("Excluded venue names:", ", ".join(sorted(excluded_venues)))

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

roller_min_date = roller_df["DATE"].min()
roller_max_date = roller_df["DATE"].max()
print(f"Detected Roller date range: {roller_min_date} through {roller_max_date}")

# Get the latest available date in Snowflake
print("Getting latest Snowflake date...")
latest_snowflake_date = get_latest_snowflake_date(conn_params)
print(f"Latest Snowflake date: {latest_snowflake_date}")

if latest_snowflake_date and pd.to_datetime(roller_max_date).strftime("%Y-%m-%d") > latest_snowflake_date:
    print(f"WARNING: Roller latest date ({roller_max_date}) is newer than Snowflake data ({latest_snowflake_date})")
    print("Snowflake may not contain the latest Roller dates.")

print(f"Fetching Snowflake revenue for Roller dates {roller_min_date} through {roller_max_date}...")
snowflake_df = load_snowflake_data(
    conn_params,
    roller_df["DATE"].unique(),
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

notify_if_revenue_discrepancy(
    result=result,
    output_file=output_file,
    webhook_url=TEAMS_WEBHOOK_URL,
    threshold=TOTAL_REVENUE_VARIANCE_THRESHOLD,
    include_excel_file=TEAMS_INCLUDE_EXCEL_FILE,
)

if OPEN_REPORT_AFTER_RUN and os.path.exists(output_file):
    os.startfile(output_file)
