import os
import zipfile

import pandas as pd

REQUIRED_COLUMNS = ["DATE", "VENUE", "ROLLER_REVENUE"]


def load_roller_csv(file_path):
    print("Processing file:", file_path)

    csv_path = extract_csv_if_needed(file_path)
    print("Loading CSV:", csv_path)

    df = pd.read_csv(csv_path, encoding="latin1")
    print("Columns:", df.columns.tolist())

    df.columns = [column.strip() for column in df.columns]
    df = df.rename(columns=build_roller_column_map(df.columns))
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    print("Renamed Columns:", df.columns.tolist())

    validate_required_columns(df)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df["VENUE"] = normalize_venue_series(df["VENUE"])

    roller_date = df["DATE"].iloc[0]
    print("Detected Roller date:", roller_date)

    return df[REQUIRED_COLUMNS], roller_date


def extract_csv_if_needed(file_path):
    if not file_path.lower().endswith(".zip"):
        return file_path

    extract_path = os.path.dirname(file_path)
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    print("ZIP extracted")
    csv_files = []
    for root, _, files in os.walk(extract_path):
        for file_name in files:
            if file_name.lower().endswith(".csv"):
                csv_files.append(os.path.join(root, file_name))

    if not csv_files:
        raise ValueError("No CSV found after extracting ZIP")

    return max(csv_files, key=os.path.getctime)


def build_roller_column_map(columns):
    rename_map = {}

    for column in columns:
        column_lower = column.lower()
        if "revenue" in column_lower:
            rename_map[column] = "ROLLER_REVENUE"
        elif "venue" in column_lower:
            rename_map[column] = "VENUE"
        elif "date" in column_lower:
            rename_map[column] = "DATE"

    return rename_map


def validate_required_columns(df):
    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing Roller column(s): {', '.join(missing_columns)}")


def normalize_venue_series(series):
    return (
        series.astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
