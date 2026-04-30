import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path


def check_dependencies():
    missing = []

    required_modules = {
        "pandas": "pandas",
        "selenium": "selenium",
        "webdriver_manager": "webdriver-manager",
        "snowflake.connector": "snowflake-connector-python",
        "cryptography": "cryptography",
        "openpyxl": "openpyxl",
    }

    for module_name, package_name in required_modules.items():
        try:
            __import__(module_name)
        except ImportError:
            missing.append(package_name)

    if missing:
        print("Missing required Python packages:")
        for package in missing:
            print(f"  - {package}")
        print("\nInstall them with:")
        print("  python -m pip install -r requirements.txt")
        sys.exit(1)


check_dependencies()

import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import Encoding
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


BASE_DIR = Path(__file__).resolve().parent
SOURCE_KEY_PATH = BASE_DIR / "rsa_key.p8"

URL = "https://my.roller.app/u/login"
DASHBOARD_URL = "https://manage.haveablast.roller.app/analytics/manage/dashboard"

ROLLER_USERNAME = "dataopsedw2@skyzone.com"
ROLLER_PASSWORD = "mG5670dFDgPSFF7"

SNOWFLAKE_CONFIG = {
    "user": "DEV_SVCCONNECTION",
    "account": "pk81200.west-us-2.azure",
    "warehouse": "DEV_DATASTRATEGY_XSMALL",
    "database": "GOLD_DB",
    "schema": "DW",
    "private_key_passphrase": "le9beb2mab9",
}

PAGE_LOAD_TIMEOUT = 60
SCRIPT_TIMEOUT = 30
ROLLER_RETRIES = 3


def create_temp_workspace():
    temp_root = Path(tempfile.mkdtemp(prefix="sz_rev_valid_"))
    paths = {
        "root": temp_root,
        "parks": temp_root / "parks",
        "roller_downloads": temp_root / "roller_downloads",
        "output": temp_root / "output",
        "key": temp_root / "key" / "rsa_key.p8",
    }

    for folder in [paths["parks"], paths["roller_downloads"], paths["output"], paths["key"].parent]:
        folder.mkdir(parents=True, exist_ok=True)

    if not SOURCE_KEY_PATH.exists():
        raise FileNotFoundError(f"Private key not found: {SOURCE_KEY_PATH}")

    shutil.copy2(SOURCE_KEY_PATH, paths["key"])
    return paths


def get_connection(conn_params, key_path):
    with open(key_path, "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=conn_params["private_key_passphrase"].encode("utf-8"),
            backend=default_backend(),
        )

    private_key_bytes = p_key.private_bytes(
        encoding=Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        user=conn_params["user"],
        account=conn_params["account"],
        warehouse=conn_params["warehouse"],
        database=conn_params["database"],
        schema=conn_params["schema"],
        private_key=private_key_bytes,
    )


def fetch_active_parks(conn_params, key_path, parks_folder):
    conn = get_connection(conn_params, key_path)

    query = """
    SELECT ROLLERNAME
    FROM DIMLOCATION
    WHERE BUSINESSGROUP = 'O&O'
      AND (CLOSEDATE IS NULL OR CLOSEDATE > CURRENT_DATE())
    ORDER BY ROLLERNAME
    """

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    parks_file = parks_folder / "parks_list.csv"
    df.to_csv(parks_file, index=False)
    print("Parks list saved temporarily at:", parks_file)

    return df["ROLLERNAME"].dropna().tolist()


def get_latest_snowflake_date(conn_params, key_path):
    conn = get_connection(conn_params, key_path)

    query = """
    SELECT MAX(RECORDDATE) as latest_date
    FROM FACTREVENUE
    """

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    if not df.empty:
        latest = df.iloc[0, 0]
        if latest is not None:
            return pd.to_datetime(latest).strftime("%Y-%m-%d")
    return None


def quote_sql(value):
    return str(value).replace("'", "''")


def load_snowflake_data(conn_params, key_path, check_date, parks_list):
    conn = get_connection(conn_params, key_path)
    park_string = ",".join([f"'{quote_sql(p)}'" for p in parks_list])

    print("Fetching Snowflake data for date:", check_date)
    print("Total parks passed:", len(parks_list))

    query = f"""
    SELECT
        CAST(fr.recorddate AS DATE) AS DATE,
        dl.rollername AS VENUE,
        SUM(fr.netrevenue) AS SNOWFLAKE_REVENUE
    FROM FACTREVENUE fr
    JOIN DIMLOCATION dl ON fr.sk_location = dl.sk_location
    WHERE dl.rollername IN ({park_string})
        AND CAST(fr.recorddate AS DATE) = TO_DATE('{check_date}')
    GROUP BY CAST(fr.recorddate AS DATE), dl.rollername
    ORDER BY dl.rollername
    """

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    print("Rows fetched from Snowflake:", len(df))

    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
        df["VENUE"] = normalize_venue(df["VENUE"])
    else:
        print("WARNING: No data returned from Snowflake")

    return df


def start_driver(download_path):
    chrome_options = Options()

    prefs = {
        "download.default_directory": str(Path(download_path).resolve()),
        "download.prompt_for_download": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options,
    )
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(SCRIPT_TIMEOUT)
    driver.maximize_window()
    return driver


def safe_get(driver, url, label, attempts=2):
    for attempt in range(1, attempts + 1):
        try:
            print(f"Opening {label} (attempt {attempt}/{attempts})...")
            driver.get(url)
            return
        except TimeoutException:
            print(f"{label} took too long to load.")
            try:
                driver.execute_script("window.stop();")
            except WebDriverException:
                pass
        except WebDriverException as e:
            print(f"{label} failed to load: {e}")

        if attempt < attempts:
            time.sleep(5)
            try:
                driver.refresh()
            except WebDriverException:
                pass

    raise TimeoutException(f"Unable to load {label} after {attempts} attempts")


def wait_for(driver, condition, description, timeout=40):
    try:
        return WebDriverWait(driver, timeout).until(condition)
    except TimeoutException as e:
        raise TimeoutException(f"Timed out waiting for {description}") from e


def wait_for_download_and_rename(download_path, timeout=120):
    print("Waiting for download...")
    start_time = time.time()
    download_path = Path(download_path)

    while True:
        files = [
            item for item in download_path.iterdir()
            if item.is_file() and not item.name.endswith(".crdownload")
        ]

        if files:
            latest = max(files, key=lambda item: item.stat().st_ctime)
            ext = latest.suffix or ".csv"
            final_path = download_path / f"roller_data{ext}"

            if final_path.exists():
                try:
                    final_path.unlink()
                except PermissionError:
                    pass

            try:
                latest.rename(final_path)
            except OSError:
                shutil.copy2(latest, final_path)
                latest.unlink()

            print("Saved temporary Roller file:", final_path)
            return str(final_path)

        if time.time() - start_time > timeout:
            raise TimeoutException("Download timeout")

        time.sleep(2)


def download_dashboard_once(download_path):
    driver = start_driver(download_path)

    try:
        safe_get(driver, URL, "login page")

        wait_for(
            driver,
            EC.presence_of_element_located((By.ID, "username")),
            "login username field",
        ).send_keys(ROLLER_USERNAME)
        driver.find_element(By.ID, "password").send_keys(ROLLER_PASSWORD)
        driver.find_element(By.NAME, "action").click()

        time.sleep(6)

        safe_get(driver, DASHBOARD_URL, "dashboard page")
        time.sleep(8)

        wait_for(
            driver,
            EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe")),
            "dashboard iframe",
        )

        dashboard = wait_for(
            driver,
            EC.element_to_be_clickable(
                (By.XPATH, "//*[contains(text(),'Revenue By Park By Day')]")
            ),
            "Revenue By Park By Day dashboard",
        )
        dashboard.click()
        time.sleep(10)

        refresh_button = find_refresh_button(driver)
        if not refresh_button:
            raise Exception("Could not find refresh button")

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", refresh_button)
        driver.execute_script("arguments[0].click();", refresh_button)
        print("Clicked refresh button")

        print("Waiting for data to refresh...")
        time.sleep(5)

        print("Clicking Dashboard actions button...")
        dashboard_actions = wait_for(
            driver,
            EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Dashboard actions')]")),
            "Dashboard actions button",
        )
        dashboard_actions.click()
        time.sleep(1)

        print("Clicking Download...")
        download_btn = wait_for(
            driver,
            EC.element_to_be_clickable((By.XPATH, "//span[contains(.,'Download')]")),
            "Download menu item",
        )
        download_btn.click()
        time.sleep(2)

        print("Waiting for download dialog...")
        format_box = wait_for(
            driver,
            EC.element_to_be_clickable((By.XPATH, "//input[@name='formatOption']")),
            "format selector",
        )

        print("Selecting CSV...")
        driver.execute_script("arguments[0].click();", format_box)
        time.sleep(1)
        format_box.send_keys(Keys.CONTROL + "a")
        format_box.send_keys(Keys.DELETE)
        time.sleep(0.5)
        format_box.send_keys("CSV")
        time.sleep(1)

        try:
            csv_option = wait_for(
                driver,
                EC.element_to_be_clickable((By.XPATH, "//li[contains(.,'CSV')]")),
                "CSV option",
            )
            csv_option.click()
        except TimeoutException:
            format_box.send_keys(Keys.ENTER)

        wait_for(
            driver,
            lambda d: d.find_element(
                By.XPATH,
                "//input[@name='formatOption']",
            ).get_attribute("value").strip().upper() == "CSV",
            "CSV selection",
        )
        print("CSV confirmed selected")

        print("Clicking CSV Download...")
        buttons = driver.find_elements(By.XPATH, "//button[normalize-space()='Download']")
        for btn in reversed(buttons):
            try:
                if btn.is_displayed() and btn.is_enabled():
                    driver.execute_script("arguments[0].click();", btn)
                    print("Clicked CSV Download button")
                    break
            except WebDriverException:
                pass
        else:
            raise Exception("CSV Download button not found")

        time.sleep(10)
        file_path = wait_for_download_and_rename(download_path)
        time.sleep(5)
        return file_path
    finally:
        try:
            driver.quit()
        except WebDriverException:
            pass


def find_refresh_button(driver):
    print("Searching for refresh button...")

    svg_elements = driver.find_elements(By.XPATH, "//svg[contains(@d, 'M17.65')]")
    for svg in svg_elements:
        try:
            parent = svg.find_element(By.XPATH, "./ancestor::button[1]")
            if parent.is_displayed() and parent.is_enabled():
                return parent
        except WebDriverException:
            pass

    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for iframe in iframes:
        try:
            driver.switch_to.frame(iframe)

            buttons = driver.find_elements(By.XPATH, "//button[.//svg[contains(@d, 'M17.65')]]")
            for btn in buttons:
                try:
                    if btn.is_displayed() and btn.is_enabled():
                        return btn
                except WebDriverException:
                    pass

            buttons = driver.find_elements(By.XPATH, "//button[@aria-labelledby='page-freshness-indicator']")
            for btn in buttons:
                try:
                    if btn.is_displayed() and btn.is_enabled():
                        return btn
                except WebDriverException:
                    pass

            driver.switch_to.default_content()
        except WebDriverException:
            try:
                driver.switch_to.default_content()
            except WebDriverException:
                pass

    return None


def download_dashboard(download_path, retries=ROLLER_RETRIES):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Starting Roller download attempt {attempt}/{retries}...")
            return download_dashboard_once(download_path)
        except Exception as e:
            last_error = e
            print(f"Roller download attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                print("Restarting browser and trying Roller again...")
                time.sleep(10)

    raise Exception(f"Roller download failed after {retries} attempts") from last_error


def load_roller_csv(file_path):
    print("Processing Roller file:", file_path)
    file_path = Path(file_path)

    if file_path.suffix.lower() == ".zip":
        extract_path = file_path.parent / file_path.stem
        extract_path.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        csv_files = list(extract_path.rglob("*.csv"))
        if not csv_files:
            raise Exception("No CSV found after extracting Roller ZIP")

        file_path = max(csv_files, key=lambda item: item.stat().st_ctime)

    print("Loading Roller CSV:", file_path)
    df = pd.read_csv(file_path, encoding="latin1")
    df.columns = [col.strip() for col in df.columns]

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
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")

    required = ["DATE", "VENUE", "ROLLER_REVENUE"]
    for col in required:
        if col not in df.columns:
            raise Exception(f"Missing Roller column: {col}")

    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df["VENUE"] = normalize_venue(df["VENUE"])

    roller_date = df["DATE"].iloc[0]
    print("Detected Roller date:", roller_date)

    return df[required], roller_date


def normalize_venue(series):
    return (
        series
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def compare_revenue(roller_df, snowflake_df):
    roller_df = roller_df.copy()
    snowflake_df = snowflake_df.copy()

    roller_df["DATE"] = pd.to_datetime(roller_df["DATE"])
    snowflake_df["DATE"] = pd.to_datetime(snowflake_df["DATE"])

    roller_df["ROLLER_REVENUE"] = (
        roller_df["ROLLER_REVENUE"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    snowflake_df["SNOWFLAKE_REVENUE"] = pd.to_numeric(
        snowflake_df["SNOWFLAKE_REVENUE"],
        errors="coerce",
    )

    common = set(roller_df["VENUE"]) & set(snowflake_df["VENUE"])
    print("Roller venues:", len(roller_df["VENUE"].unique()))
    print("Snowflake venues:", len(snowflake_df["VENUE"].unique()))
    print("Matching venues:", len(common))

    merged = pd.merge(
        roller_df,
        snowflake_df,
        on=["DATE", "VENUE"],
        how="outer",
    )
    merged = merged.fillna(0)
    merged["VARIANCE"] = merged["SNOWFLAKE_REVENUE"] - merged["ROLLER_REVENUE"]
    merged["MATCH"] = merged["VARIANCE"].round(2).apply(
        lambda value: "Match" if value == 0 else "Mismatch"
    )

    return merged


def write_report(result, output_folder):
    output_file = Path(output_folder) / "revenue_comparison.xlsx"
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

    print("Report saved temporarily at:", output_file)
    return output_file


def open_excel_and_cleanup_later(output_file, temp_root):
    output_file = Path(output_file).resolve()
    temp_root = Path(temp_root).resolve()

    if os.name == "nt":
        os.startfile(output_file)
        start_windows_cleanup_watcher(output_file, temp_root)
        print("Excel opened. Temporary files will be removed after the workbook is closed.")
        return

    print("Report is ready:", output_file)
    print("Close the file before deleting the temp folder:", temp_root)


def start_windows_cleanup_watcher(output_file, temp_root):
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if not powershell:
        print("Could not find PowerShell for background cleanup.")
        print("Temporary folder:", temp_root)
        return

    command = f"""
$file = '{escape_powershell_path(output_file)}'
$folder = '{escape_powershell_path(temp_root)}'
Start-Sleep -Seconds 10
while (Test-Path -LiteralPath $file) {{
    try {{
        $stream = [System.IO.File]::Open($file, 'Open', 'ReadWrite', 'None')
        $stream.Close()
        break
    }} catch {{
        Start-Sleep -Seconds 5
    }}
}}
Remove-Item -LiteralPath $folder -Recurse -Force -ErrorAction SilentlyContinue
"""

    subprocess.Popen(
        [
            powershell,
            "-NoProfile",
            "-WindowStyle",
            "Hidden",
            "-Command",
            command,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )


def escape_powershell_path(path):
    return str(path).replace("'", "''")


def cleanup_temp_now(temp_root):
    try:
        shutil.rmtree(temp_root)
    except PermissionError:
        print("Temporary folder is still in use and was not removed:", temp_root)


def run_pipeline():
    paths = create_temp_workspace()
    print("Created temporary workspace:", paths["root"])

    try:
        print("Using temporary key file:", paths["key"])
        print("Fetching parks from Snowflake...")
        parks_list = fetch_active_parks(SNOWFLAKE_CONFIG, paths["key"], paths["parks"])
        print("Total parks:", len(parks_list))

        print("Opening Roller...")
        roller_file = download_dashboard(paths["roller_downloads"])

        print("Loading Roller CSV...")
        roller_df = None
        check_date = None
        for attempt in range(1, 6):
            try:
                roller_df, check_date = load_roller_csv(roller_file)
                break
            except PermissionError:
                print(f"File access denied, retrying ({attempt}/5)...")
                time.sleep(2)

        if roller_df is None:
            raise Exception("Unable to load Roller CSV")

        roller_date = pd.to_datetime(check_date).strftime("%Y-%m-%d")

        print("Getting latest Snowflake date...")
        latest_snowflake_date = get_latest_snowflake_date(SNOWFLAKE_CONFIG, paths["key"])
        print("Latest Snowflake date:", latest_snowflake_date)

        if latest_snowflake_date and roller_date > latest_snowflake_date:
            print(f"WARNING: Roller date ({roller_date}) is newer than Snowflake data ({latest_snowflake_date})")
            check_date = latest_snowflake_date
        else:
            check_date = roller_date

        print("Using comparison date:", check_date)

        print("Fetching Snowflake revenue...")
        snowflake_df = load_snowflake_data(
            SNOWFLAKE_CONFIG,
            paths["key"],
            check_date,
            parks_list,
        )

        if snowflake_df.empty and latest_snowflake_date:
            check_date = latest_snowflake_date
            print("Retrying Snowflake revenue with latest date:", check_date)
            snowflake_df = load_snowflake_data(
                SNOWFLAKE_CONFIG,
                paths["key"],
                check_date,
                parks_list,
            )

        print("Comparing revenue...")
        result = compare_revenue(roller_df, snowflake_df)

        output_file = write_report(result, paths["output"])
        open_excel_and_cleanup_later(output_file, paths["root"])
    except Exception:
        cleanup_temp_now(paths["root"])
        raise


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:
        print("\nFAILED:", exc)
        sys.exit(1)
