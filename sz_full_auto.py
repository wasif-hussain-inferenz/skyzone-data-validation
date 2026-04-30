# ============================================================================
# SkyZone Full Automation - Single File Solution
# ============================================================================
# This script runs the complete end-to-end revenue comparison pipeline:
# 1. Fetch active parks from Snowflake
# 2. Download revenue data from Roller (automated web scraping)
# 3. Load and process Roller CSV
# 4. Fetch revenue from Snowflake
# 5. Compare and generate Excel report
# ============================================================================

import os
import time
import zipfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import snowflake.connector
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

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
# OPTION 2: External Browser Authentication (current)
# ============================================================
# SNOWFLAKE_CONFIG = {
#     "user": "MOHAMMED.HUSSAIN.IO@SKYZONE.COM",
#     "account": "BL89401-SKYZONE_QA",
#     "warehouse": "QA_DATASTRATEGY_XSMALL",
#     "database": "GOLD_DB",
#     "schema": "DW",
#     "authenticator": "externalbrowser"
# }

# Roller Configuration
ROLLER_URL = "https://my.roller.app/u/login"
ROLLER_DASHBOARD_URL = "https://manage.haveablast.roller.app/analytics/manage/dashboard"
ROLLER_USERNAME = "dataopsedw2@skyzone.com"
ROLLER_PASSWORD = "mG5670dFDgPSFF7"

# Paths
ROLLER_DOWNLOAD_PATH = os.path.join(os.path.dirname(__file__), "data", "downloads")
REPORT_PATH = os.path.join(os.path.dirname(__file__), "data", "output")

# ============================================================================
# SNOWFLAKE FUNCTIONS
# ============================================================================

def get_snowflake_connection():
    """Create Snowflake connection - supports both key-based and externalbrowser auth"""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import Encoding
    from cryptography.hazmat.backends import default_backend
    
    # Check if using key-based authentication
    if "private_key_path" in SNOWFLAKE_CONFIG and SNOWFLAKE_CONFIG.get("private_key_path"):
        # Key-based authentication
        key_file_path = SNOWFLAKE_CONFIG.get("private_key_path", "rsa_key.p8")
        
        # If path is not absolute, make it relative to script location
        if not os.path.isabs(key_file_path):
            key_file_path = os.path.join(os.path.dirname(__file__), key_file_path)
        
        # Read and decrypt the private key
        with open(key_file_path, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=SNOWFLAKE_CONFIG["private_key_passphrase"].encode('utf-8'),
                backend=default_backend()
            )
        
        # Convert to bytes (use DER format)
        private_key_bytes = p_key.private_bytes(
            encoding=Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        return snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            account=SNOWFLAKE_CONFIG["account"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
            private_key=private_key_bytes,
        )
    
    # Otherwise use externalbrowser authentication
    else:
        chrome_path = os.environ.get(
            "SNOWFLAKE_BROWSER",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        )

        if not Path(chrome_path).exists():
            chrome_path = "chrome"

        os.environ["BROWSER"] = chrome_path

        return snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG["user"],
            account=SNOWFLAKE_CONFIG["account"],
            warehouse=SNOWFLAKE_CONFIG["warehouse"],
            database=SNOWFLAKE_CONFIG["database"],
            schema=SNOWFLAKE_CONFIG["schema"],
            authenticator=SNOWFLAKE_CONFIG.get("authenticator", "externalbrowser")
        )

def fetch_active_parks():
    """Fetch active park names from Snowflake"""
    print("Fetching parks from Snowflake...")
    conn = get_snowflake_connection()
    
    query = """
    SELECT ROLLERNAME
    FROM DIMLOCATION
    WHERE BUSINESSGROUP = 'O&O'
      AND (CLOSEDATE IS NULL OR CLOSEDATE > CURRENT_DATE())
    ORDER BY ROLLERNAME
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Total parks: {len(df)}")
    return df["ROLLERNAME"].dropna().tolist()

def load_snowflake_data(check_date, parks_list):
    """Fetch revenue data from Snowflake"""
    print("Fetching Snowflake revenue...")
    conn = get_snowflake_connection()
    
    park_string = ",".join([f"'{p}'" for p in parks_list])
    
    query = f"""
    WITH base AS (
        SELECT 
            dl.rollername,
            CAST(
                DATEADD(
                    hour, 
                    COALESCE(dl.OFFSETHOURSTOMATCHROLLERWEBSITE, 0),
                    CONVERT_TIMEZONE('America/New_York', fr.recorddate)
                ) AS DATE
            ) AS venue_manager_date,
            fr.netrevenue
        FROM FACTREVENUE fr
        JOIN DIMLOCATION dl ON fr.sk_location = dl.sk_location
    )

    SELECT 
        b.venue_manager_date AS DATE,
        b.rollername AS VENUE,
        SUM(b.netrevenue) AS SNOWFLAKE_REVENUE
    FROM base b
    WHERE b.rollername IN ({park_string})
        AND b.venue_manager_date = TO_DATE('{check_date}')
    GROUP BY b.venue_manager_date, b.rollername
    ORDER BY b.rollername
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"Snowflake revenue records: {len(df)}")
    return df

# ============================================================================
# ROLLER DOWNLOADER FUNCTIONS
# ============================================================================

def start_driver():
    """Start Chrome driver with download settings"""
    chrome_options = Options()
    prefs = {
        "download.default_directory": os.path.abspath(ROLLER_DOWNLOAD_PATH),
        "download.prompt_for_download": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.maximize_window()
    return driver

def wait_for_download_and_rename(download_path, timeout=120):
    """Wait for download to complete and rename file"""
    print("Waiting for download...")
    start_time = time.time()
    
    while True:
        files = os.listdir(download_path)
        files = [f for f in files if not f.endswith(".crdownload")]
        
        if files:
            latest = max(
                [os.path.join(download_path, f) for f in files],
                key=os.path.getctime
            )
            ext = os.path.splitext(latest)[1]
            if not ext:
                ext = ".csv"
            
            final_path = os.path.join(download_path, f"roller_data{ext}")
            
            if os.path.exists(final_path):
                try:
                    os.remove(final_path)
                except:
                    pass
            
            try:
                os.rename(latest, final_path)
            except:
                import shutil
                shutil.copy2(latest, final_path)
                os.remove(latest)
            
            print(f"Saved: {final_path}")
            return final_path
        
        if time.time() - start_time > timeout:
            raise Exception("Download timeout")
        
        time.sleep(2)

def download_dashboard():
    """Main function to automate Roller dashboard download"""
    driver = start_driver()
    wait = WebDriverWait(driver, 40)
    
    print("Opening Roller login...")
    driver.get(ROLLER_URL)
    
    wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(ROLLER_USERNAME)
    driver.find_element(By.ID, "password").send_keys(ROLLER_PASSWORD)
    driver.find_element(By.NAME, "action").click()
    
    time.sleep(6)
    
    print("Navigating to dashboard...")
    driver.get(ROLLER_DASHBOARD_URL)
    time.sleep(8)
    
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe")))
    
    # Click on Revenue By Park By Day dashboard
    dashboard = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//*[contains(text(),'Revenue By Park By Day')]")
        )
    )
    dashboard.click()
    print("Clicked Revenue By Park By Day")
    
    time.sleep(10)
    
    # Find and click refresh button
    print("Looking for refresh button...")
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    refresh_button = None
    
    for idx, iframe in enumerate(iframes):
        try:
            driver.switch_to.frame(iframe)
            time.sleep(1)
            buttons = driver.find_elements(By.XPATH, "//button[@aria-labelledby='page-freshness-indicator']")
            for btn in buttons:
                if btn.is_displayed() and btn.is_enabled():
                    refresh_button = btn
                    break
            if refresh_button:
                break
            driver.switch_to.default_content()
        except:
            driver.switch_to.default_content()
    
    if refresh_button:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", refresh_button)
        driver.execute_script("arguments[0].click();", refresh_button)
        print("Clicked refresh button!")
    
    time.sleep(5)
    
    # Click Dashboard actions button
    print("Clicking Dashboard actions...")
    try:
        dashboard_actions = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(.,'Dashboard actions')]")
            )
        )
        dashboard_actions.click()
        time.sleep(1)
    except Exception as e:
        print(f"Dashboard actions not found: {e}")
    
    # Click Download
    print("Clicking Download...")
    try:
        download_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[contains(.,'Download')]")
            )
        )
        download_btn.click()
        time.sleep(2)
    except Exception as e:
        print(f"Download button not found: {e}")
    
    # Wait for Format label
    print("Waiting for download dialog...")
    try:
        wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//label[contains(.,'Format')]")
            )
        )
    except Exception as e:
        print(f"Format label not found: {e}")
    
    # Select CSV format
    print("Selecting CSV format...")
    try:
        format_box = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//input[@name='formatOption']")
            )
        )
        driver.execute_script("arguments[0].click();", format_box)
        time.sleep(1)
        
        format_box.send_keys(Keys.CONTROL + "a")
        format_box.send_keys(Keys.DELETE)
        time.sleep(0.5)
        
        format_box.send_keys("CSV")
        time.sleep(1)
        
        # Try clicking CSV option
        try:
            csv_option = wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[contains(.,'CSV')]")
                )
            )
            csv_option.click()
        except:
            format_box.send_keys(Keys.ENTER)
        
        time.sleep(1)
    except Exception as e:
        print(f"Could not select CSV: {e}")
    
    # Click final Download button
    print("Starting download...")
    try:
        buttons = driver.find_elements(By.XPATH, "//button[normalize-space()='Download']")
        for btn in reversed(buttons):
            if btn.is_displayed() and btn.is_enabled():
                driver.execute_script("arguments[0].click();", btn)
                print("Clicked Download button")
                break
    except Exception as e:
        print(f"Download button not found: {e}")
    
    time.sleep(20)
    
    file_path = wait_for_download_and_rename(ROLLER_DOWNLOAD_PATH)
    time.sleep(5)
    
    driver.quit()
    return file_path

# ============================================================================
# ROLLER CSV LOADER FUNCTIONS
# ============================================================================

def load_roller_csv(file_path):
    """Load and process Roller CSV file"""
    print(f"Loading CSV: {file_path}")
    
    csv_file = file_path
    
    # Handle zip files
    if file_path.endswith(".zip"):
        print("Extracting zip file...")
        extract_dir = file_path.replace(".zip", "")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(file_path))
        print(f"Extracted to: {extract_dir}")
        
        # Find the actual CSV file inside the extracted directory
        csv_files = []
        if os.path.isdir(extract_dir):
            for f in os.listdir(extract_dir):
                if f.endswith(".csv"):
                    csv_files.append(os.path.join(extract_dir, f))
        
        if csv_files:
            # Use the first CSV file found
            csv_file = csv_files[0]
            print(f"Found CSV file: {csv_file}")
        else:
            # Fallback to old behavior
            csv_file = extract_dir
    
    # Try different encodings
    df = None
    for encoding in ["latin1", "utf-8", "cp1252"]:
        try:
            df = pd.read_csv(csv_file, encoding=encoding)
            print(f"Successfully loaded with encoding: {encoding}")
            break
        except Exception as e:
            print(f"Failed with {encoding}: {e}")
            continue
    
    if df is None:
        raise Exception(f"Could not load CSV file: {csv_file}")
    
    # Find date column
    date_col = None
    for col in df.columns:
        if "date" in col.lower() or "day" in col.lower():
            date_col = col
            break
    
    if date_col:
        check_date = df[date_col].max()
    else:
        check_date = pd.Timestamp.now()
    
    print(f"Roller records: {len(df)}")
    print(f"Check date: {check_date}")
    
    return df, check_date

# ============================================================================
# REVENUE COMPARE FUNCTIONS
# ============================================================================

def compare_revenue(roller_df, snowflake_df):
    """Compare Roller and Snowflake revenue data"""
    print("Comparing revenue...")
    
    # Normalize column names
    roller_df = roller_df.copy()
    snowflake_df = snowflake_df.copy()
    
    # Clean up Roller dataframe - remove unnamed index column if exists
    if roller_df.columns[0] == '' or roller_df.columns[0].startswith('Unnamed'):
        roller_df = roller_df.iloc[:, 1:]
    
    # Find park column in Roller data
    roller_park_col = None
    for col in roller_df.columns:
        if "park" in col.lower() or "location" in col.lower() or "name" in col.lower() or "venue" in col.lower():
            roller_park_col = col
            break
    
    # Find revenue column in Roller data
    roller_rev_col = None
    for col in roller_df.columns:
        if "revenue" in col.lower() or "amount" in col.lower() or "total" in col.lower():
            roller_rev_col = col
            break
    
    if not roller_park_col or not roller_rev_col:
        print("Roller columns:", roller_df.columns.tolist())
        raise ValueError("Could not find park or revenue column in Roller data")
    
    # Clean revenue column in Roller (remove $ and commas)
    roller_df[roller_rev_col] = roller_df[roller_rev_col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
    roller_df[roller_rev_col] = pd.to_numeric(roller_df[roller_rev_col], errors='coerce')
    
    # Convert park column to string for consistent merging
    roller_df[roller_park_col] = roller_df[roller_park_col].astype(str).str.strip()
    
    # Aggregate Roller data by park
    roller_agg = roller_df.groupby(roller_park_col)[roller_rev_col].sum().reset_index()
    roller_agg.columns = ["PARK", "ROLLER_REVENUE"]
    
    # Find park column in Snowflake data
    snow_park_col = None
    for col in snowflake_df.columns:
        if "park" in col.lower() or "venue" in col.lower() or "location" in col.lower() or "name" in col.lower():
            snow_park_col = col
            break
    
    # Find revenue column in Snowflake data
    snow_rev_col = None
    for col in snowflake_df.columns:
        if "revenue" in col.lower() or "amount" in col.lower() or "total" in col.lower():
            snow_rev_col = col
            break
    
    if not snow_park_col or not snow_rev_col:
        print("Snowflake columns:", snowflake_df.columns.tolist())
        raise ValueError("Could not find park or revenue column in Snowflake data")
    
    # Convert park column to string for consistent merging
    snowflake_df[snow_park_col] = snowflake_df[snow_park_col].astype(str).str.strip()
    
    # Aggregate Snowflake data by park
    snowflake_agg = snowflake_df.groupby(snow_park_col)[snow_rev_col].sum().reset_index()
    snowflake_agg.columns = ["PARK", "SNOWFLAKE_REVENUE"]
    
    # Merge
    result = pd.merge(roller_agg, snowflake_agg, on="PARK", how="outer")
    result = result.fillna(0)
    
    # Calculate difference
    result["DIFFERENCE"] = result["ROLLER_REVENUE"] - result["SNOWFLAKE_REVENUE"]
    result["MATCH"] = result["DIFFERENCE"].apply(lambda x: "Match" if abs(x) < 1 else "Mismatch")
    
    return result

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("SKYZONE REVENUE AUTOMATION - FULL PIPELINE")
    print("=" * 60)
    
    # Step 1: Fetch parks from Snowflake
    parks_list = fetch_active_parks()
    
    # Step 2: Download Roller data
    print("\nOpening Roller for download...")
    roller_file = download_dashboard()
    
    # Step 3: Load Roller CSV
    print("\nLoading Roller CSV...")
    for attempt in range(5):
        try:
            roller_df, check_date = load_roller_csv(roller_file)
            break
        except PermissionError as e:
            print(f"File access denied, retrying ({attempt + 1}/5)...")
            time.sleep(2)
    
    check_date = pd.to_datetime(check_date).strftime('%Y-%m-%d')
    print(f"Using date: {check_date}")
    
    # Step 4: Fetch Snowflake revenue
    snowflake_df = load_snowflake_data(check_date, parks_list)
    
    # Step 5: Compare and generate report
    print("\nComparing revenue...")
    result = compare_revenue(roller_df, snowflake_df)
    
    # Step 6: Save to Excel
    os.makedirs(REPORT_PATH, exist_ok=True)
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
    
    print(f"\n{'=' * 60}")
    print(f"Report saved at: {output_file}")
    print(f"{'=' * 60}")
    
    # Open the file
    if os.path.exists(output_file):
        os.startfile(output_file)

if __name__ == "__main__":
    main()