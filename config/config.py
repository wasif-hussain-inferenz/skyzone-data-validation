import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ROLLER_DOWNLOAD_PATH = os.getenv(
    "ROLLER_DOWNLOAD_PATH",
    os.path.join(BASE_DIR, "data", "downloads"),
)
INPUT_PATH = os.getenv("INPUT_PATH", os.path.join(BASE_DIR, "data", "input"))
REPORT_PATH = os.getenv("REPORT_PATH", os.path.join(BASE_DIR, "data", "output"))
REPORT_FILE_NAME = os.getenv("REPORT_FILE_NAME", "revenue_comparison.xlsx")


def _env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name, default):
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _env_float(name, default):
    value = os.getenv(name)
    return float(value) if value not in (None, "") else default


def _env_csv(name, default):
    return [
        " ".join(item.upper().split())
        for item in os.getenv(name, default).split(",")
        if item.strip()
    ]


TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")
TEAMS_INCLUDE_EXCEL_FILE = _env_bool("TEAMS_INCLUDE_EXCEL_FILE", True)
OPEN_REPORT_AFTER_RUN = _env_bool("OPEN_REPORT_AFTER_RUN", True)
TOTAL_REVENUE_VARIANCE_THRESHOLD = _env_float(
    "TOTAL_REVENUE_VARIANCE_THRESHOLD",
    0.01,
)
ROW_REVENUE_VARIANCE_THRESHOLD = _env_float("ROW_REVENUE_VARIANCE_THRESHOLD", 0.005)
EXCLUDED_ROLLER_VENUES = _env_csv(
    "EXCLUDED_ROLLER_VENUES",
    "CLOUDBOUND NEW ROCHELLE NY",
)

ROLLER_LOGIN_URL = os.getenv("ROLLER_LOGIN_URL", "https://my.roller.app/u/login")
ROLLER_DASHBOARD_URL = os.getenv(
    "ROLLER_DASHBOARD_URL",
    "https://manage.haveablast.roller.app/analytics/manage/dashboard",
)
ROLLER_USERNAME = os.getenv("ROLLER_USERNAME", "dataopsedw2@skyzone.com")
ROLLER_PASSWORD = os.getenv("ROLLER_PASSWORD", "mG5670dFDgPSFF7")
ROLLER_REPORT_NAME = os.getenv("ROLLER_REPORT_NAME", "Revenue By Park By Day")
PAGE_LOAD_TIMEOUT = _env_int("PAGE_LOAD_TIMEOUT", 60)
SCRIPT_TIMEOUT = _env_int("SCRIPT_TIMEOUT", 30)
ROLLER_RETRIES = _env_int("ROLLER_RETRIES", 3)
ROLLER_DOWNLOAD_TIMEOUT = _env_int("ROLLER_DOWNLOAD_TIMEOUT", 120)
CSV_LOAD_RETRIES = _env_int("CSV_LOAD_RETRIES", 5)
CSV_LOAD_RETRY_SECONDS = _env_int("CSV_LOAD_RETRY_SECONDS", 2)

os.makedirs(ROLLER_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(REPORT_PATH, exist_ok=True)

# ============================================================
# Snowflake Key-based Authentication
# ============================================================
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER", "PR_SVCCONNECTION"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "ls01637.west-us-2.azure"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "PR_DATASTRATEGY_XSMALL"),
    "role": os.getenv("SNOWFLAKE_ROLE", "PROD_READER_FR"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "GOLD_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "DW"),
    "private_key_path": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "rsa_key.p8"),
    "private_key_passphrase": os.getenv(
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
        "le9beb2mab9",
    ),
}
