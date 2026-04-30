import pandas as pd
import zipfile
import os


def load_roller_csv(file_path):

    print("Processing file:", file_path)

    # If ZIP → extract
    if file_path.endswith(".zip"):
        extract_path = os.path.dirname(file_path)

        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        print("ZIP extracted")

        # find actual CSV inside extracted files
        csv_files = []
        for root, dirs, files in os.walk(extract_path):
            for f in files:
                if f.endswith(".csv"):
                    csv_files.append(os.path.join(root, f))

        if not csv_files:
            raise Exception("No CSV found after extracting ZIP")

        file_path = max(csv_files, key=os.path.getctime)

    print("Loading CSV:", file_path)

    # read CSV
    df = pd.read_csv(file_path, encoding="latin1")

    print("Columns:", df.columns.tolist())

    # clean column names
    df.columns = [col.strip() for col in df.columns]

    # 🔥 dynamic column mapping
    rename_map = {}

    for col in df.columns:
        col_lower = col.lower()

        if "revenue" in col_lower:
            rename_map[col] = "ROLLER_REVENUE"

        elif "venue" in col_lower:
            rename_map[col] = "VENUE"

        elif "date" in col_lower:
            rename_map[col] = "DATE"

    df = df.rename(columns=rename_map)

    print("Renamed Columns:", df.columns.tolist())

    # remove junk column if exists
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    # ✅ validate required columns
    required = ["DATE", "VENUE", "ROLLER_REVENUE"]
    for col in required:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    # 🔥🔥 IMPORTANT PART (AUTO DATE DETECTION)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

    roller_date = df["DATE"].iloc[0]

    print("Detected Roller date:", roller_date)

    # optional cleanup for matching
    df["VENUE"] = (
    df["VENUE"]
    .astype(str)
    .str.upper()
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

    return df[required], roller_date