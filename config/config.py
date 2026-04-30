import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ROLLER_DOWNLOAD_PATH = os.path.join(BASE_DIR, "data", "downloads")
INPUT_PATH = os.path.join(BASE_DIR, "data", "input")
REPORT_PATH = os.path.join(BASE_DIR, "data", "output")

os.makedirs(ROLLER_DOWNLOAD_PATH, exist_ok=True)
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(REPORT_PATH, exist_ok=True)

# ============================================================
# OPTION 1: Key-based Authentication (when IP is whitelisted)
# ============================================================
SNOWFLAKE_CONFIG = {
    "user": "DEV_SVCCONNECTION",
    "account": "pk81200.west-us-2.azure",
    "warehouse": "DEV_DATASTRATEGY_XSMALL",
    "database": "GOLD_DB",
    "schema": "DW",
    "private_key_path": "rsa_key.p8",
    "private_key_passphrase": "le9beb2mab9",
}

# ============================================================
# OPTION 2: External Browser Authentication (Prod)
# ============================================================
# SNOWFLAKE_CONFIG = {
#     "user": "MOHAMMED.HUSSAIN.IO@SKYZONE.COM",
#     "account": "BL89401-SKYZONE_QA",
#     "warehouse": "QA_DATASTRATEGY_XSMALL",
#     "database": "GOLD_DB",
#     "schema": "DW",
#     "authenticator": "externalbrowser"
# }