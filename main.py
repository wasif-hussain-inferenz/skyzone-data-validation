from config.config import (
    CSV_LOAD_RETRIES,
    CSV_LOAD_RETRY_SECONDS,
    EXCLUDED_ROLLER_VENUES,
    OPEN_REPORT_AFTER_RUN,
    REPORT_FILE_NAME,
    REPORT_PATH,
    ROW_REVENUE_VARIANCE_THRESHOLD,
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


def normalize_venue_name(venue):
    return " ".join(str(venue).upper().split())


def get_comparison_parks(conn_params):
    print("Fetching parks from Snowflake...")
    parks_list = fetch_active_parks(conn_params)
    excluded_venues = set(EXCLUDED_ROLLER_VENUES)

    if excluded_venues:
        original_park_count = len(parks_list)
        parks_list = [
            park
            for park in parks_list
            if normalize_venue_name(park) not in excluded_venues
        ]
        excluded_count = original_park_count - len(parks_list)
        print(f"Excluded parks from comparison: {excluded_count}")
        print("Excluded venue names:", ", ".join(sorted(excluded_venues)))

    print("Total parks:", len(parks_list))
    return parks_list


def load_roller_file_with_retry(roller_file):
    print("Loading roller CSV...")
    last_error = None

    for attempt in range(1, CSV_LOAD_RETRIES + 1):
        try:
            return load_roller_csv(roller_file)
        except PermissionError as exc:
            last_error = exc
            print(f"File access denied, retrying ({attempt}/{CSV_LOAD_RETRIES})...")
            time.sleep(CSV_LOAD_RETRY_SECONDS)

    raise PermissionError(
        f"Unable to read Roller file after {CSV_LOAD_RETRIES} attempts: {roller_file}"
    ) from last_error


def warn_if_snowflake_lags(roller_max_date, latest_snowflake_date):
    if not latest_snowflake_date:
        return

    roller_max_date_text = pd.to_datetime(roller_max_date).strftime("%Y-%m-%d")
    if roller_max_date_text > latest_snowflake_date:
        print(
            "WARNING: Roller latest date "
            f"({roller_max_date}) is newer than Snowflake data "
            f"({latest_snowflake_date})"
        )
        print("Snowflake may not contain the latest Roller dates.")


def fetch_snowflake_revenue(conn_params, roller_df, parks_list, latest_snowflake_date):
    roller_min_date = roller_df["DATE"].min()
    roller_max_date = roller_df["DATE"].max()
    print(
        "Fetching Snowflake revenue for Roller dates "
        f"{roller_min_date} through {roller_max_date}..."
    )

    snowflake_df = load_snowflake_data(
        conn_params,
        roller_df["DATE"].unique(),
        parks_list,
    )

    if snowflake_df.empty and latest_snowflake_date:
        print("WARNING: No data returned! Trying latest Snowflake date...")
        print(f"Retrying with date: {latest_snowflake_date}")
        snowflake_df = load_snowflake_data(
            conn_params,
            latest_snowflake_date,
            parks_list,
        )

    return snowflake_df


def build_match_summary(result):
    match_summary = (
        result["MATCH"]
        .value_counts()
        .reindex(["Match", "Mismatch"], fill_value=0)
        .reset_index()
    )
    match_summary.columns = ["STATUS", "COUNT"]
    return match_summary


def write_report(result):
    output_file = os.path.join(REPORT_PATH, REPORT_FILE_NAME)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        result.to_excel(writer, sheet_name="Revenue Comparison", index=False)
        build_match_summary(result).to_excel(
            writer,
            sheet_name="Match Summary",
            index=False,
        )

    print("Report saved at:", output_file)
    return output_file


def open_report(output_file):
    if not OPEN_REPORT_AFTER_RUN:
        print("Excel report auto-open is disabled by OPEN_REPORT_AFTER_RUN.")
        return

    if not os.path.exists(output_file):
        print(f"Excel report not opened because file was not found: {output_file}")
        return

    print("Opening Excel report...")
    os.startfile(output_file)


def run_pipeline():
    print("Using Snowflake connection from config...")
    conn_params = SNOWFLAKE_CONFIG

    parks_list = get_comparison_parks(conn_params)

    print("Opening Roller...")
    roller_file = download_dashboard()

    roller_df, _ = load_roller_file_with_retry(roller_file)
    roller_min_date = roller_df["DATE"].min()
    roller_max_date = roller_df["DATE"].max()
    print(f"Detected Roller date range: {roller_min_date} through {roller_max_date}")

    print("Getting latest Snowflake date...")
    latest_snowflake_date = get_latest_snowflake_date(conn_params)
    print(f"Latest Snowflake date: {latest_snowflake_date}")
    warn_if_snowflake_lags(roller_max_date, latest_snowflake_date)

    snowflake_df = fetch_snowflake_revenue(
        conn_params,
        roller_df,
        parks_list,
        latest_snowflake_date,
    )

    print("Comparing...")
    result = compare_revenue(
        roller_df,
        snowflake_df,
        threshold=ROW_REVENUE_VARIANCE_THRESHOLD,
    )

    output_file = write_report(result)

    notify_if_revenue_discrepancy(
        result=result,
        output_file=output_file,
        webhook_url=TEAMS_WEBHOOK_URL,
        threshold=TOTAL_REVENUE_VARIANCE_THRESHOLD,
        include_excel_file=TEAMS_INCLUDE_EXCEL_FILE,
    )

    open_report(output_file)
    return output_file


if __name__ == "__main__":
    run_pipeline()
