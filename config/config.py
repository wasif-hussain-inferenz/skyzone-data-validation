import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ROLLER_DOWNLOAD_PATH = os.path.join(BASE_DIR, "data", "downloads")
INPUT_PATH = os.path.join(BASE_DIR, "data", "input")
REPORT_PATH = os.path.join(BASE_DIR, "data", "output")

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")
TEAMS_INCLUDE_EXCEL_FILE = os.getenv("TEAMS_INCLUDE_EXCEL_FILE", "true").lower() in (
    "1",
    "true",
    "yes",
)
OPEN_REPORT_AFTER_RUN = os.getenv("OPEN_REPORT_AFTER_RUN", "false").lower() in (
    "1",
    "true",
    "yes",
)
TOTAL_REVENUE_VARIANCE_THRESHOLD = float(
    os.getenv("TOTAL_REVENUE_VARIANCE_THRESHOLD", "0.01")
)
EXCLUDED_ROLLER_VENUES = [
    " ".join(venue.upper().split())
    for venue in os.getenv(
        "EXCLUDED_ROLLER_VENUES",
        "CLOUDBOUND NEW ROCHELLE NY",
    ).split(",")
    if venue.strip()
]

os.makedirs(ROLLER_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(REPORT_PATH, exist_ok=True)

# ============================================================
# Snowflake Key-based Authentication
# ============================================================
SNOWFLAKE_CONFIG = {
    "user": "PR_SVCCONNECTION",
    "account": "ls01637.west-us-2.azure",
    "warehouse": "PR_DATASTRATEGY_XSMALL",
    "role": "PROD_READER_FR",
    "database": "GOLD_DB",
    "schema": "DW",
    "private_key_path": "rsa_key.p8",
    "private_key_passphrase": "le9beb2mab9",
}
