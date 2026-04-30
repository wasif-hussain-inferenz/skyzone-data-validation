import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config.config import ROLLER_DOWNLOAD_PATH

URL = "https://my.roller.app/u/login"
DASHBOARD_URL = "https://manage.haveablast.roller.app/analytics/manage/dashboard"

USERNAME = "dataopsedw2@skyzone.com"
PASSWORD = "mG5670dFDgPSFF7"


def start_driver():
    chrome_options = Options()

    prefs = {
        "download.default_directory": os.path.abspath(ROLLER_DOWNLOAD_PATH),
        "download.prompt_for_download": False
    }

    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    driver.maximize_window()
    return driver


def wait_for_download_and_rename(download_path, timeout=120):

    print("Waiting for download...")

    start_time = time.time()

    while True:
        files = os.listdir(download_path)

        # ignore temp files
        files = [f for f in files if not f.endswith(".crdownload")]

        if files:
            latest = max(
                [os.path.join(download_path, f) for f in files],
                key=os.path.getctime
            )

            # 👇 KEEP ORIGINAL EXTENSION
            ext = os.path.splitext(latest)[1]
            
            # If no extension, default to .csv
            if not ext:
                ext = ".csv"

            final_path = os.path.join(download_path, f"roller_data{ext}")

            # remove old file if exists
            if os.path.exists(final_path):
                try:
                    os.remove(final_path)
                except:
                    pass  # File might be in use

            try:
                os.rename(latest, final_path)
            except:
                # If rename fails (cross-device), copy instead
                import shutil
                shutil.copy2(latest, final_path)
                os.remove(latest)

            print("Saved:", final_path)
            return final_path

        if time.time() - start_time > timeout:
            raise Exception("Download timeout")

        time.sleep(2)


def click_element_across_frames(driver, locators, timeout=5):
    def search_current_context():
        for locator in locators:
            elements = driver.find_elements(*locator)
            if elements:
                element = elements[0]
                if element.is_displayed() and element.is_enabled():
                    return element

        for frame in driver.find_elements(By.TAG_NAME, "iframe"):
            driver.switch_to.frame(frame)
            try:
                element = search_current_context()
                if element:
                    return element
            finally:
                driver.switch_to.parent_frame()

        return None

    end_time = time.time() + timeout
    while time.time() < end_time:
        driver.switch_to.default_content()
        element = search_current_context()
        if element:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            driver.execute_script("arguments[0].click();", element)
            return True
        time.sleep(1)

    return False


def download_dashboard():

    driver = start_driver()
    wait = WebDriverWait(driver, 40)

    print("Opening login...")
    driver.get(URL)

    wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(USERNAME)
    driver.find_element(By.ID, "password").send_keys(PASSWORD)
    driver.find_element(By.NAME, "action").click()

    time.sleep(6)

    driver.get(DASHBOARD_URL)
    time.sleep(8)

    wait.until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe")))

    dashboard = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//*[contains(text(),'Revenue By Park By Day')]")
        )
    )
    dashboard.click()

    # Wait longer for page to fully load after clicking dashboard
    time.sleep(10)

    # Debug: Print all iframes and buttons to understand the page structure
    print("Debug: Checking page structure...")
    
    # Stay in the current iframe context (don't switch)
    
    # Check for iframes
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"Found {len(iframes)} iframes")
    
    # Debug: Find all buttons on the page
    all_buttons = driver.find_elements(By.TAG_NAME, "button")
    print(f"Found {len(all_buttons)} buttons on page")
    for i, btn in enumerate(all_buttons[:10]):  # Print first 10 buttons
        try:
            print(f"  Button {i}: class={btn.get_attribute('class')}, aria-labelledby={btn.get_attribute('aria-labelledby')}")
        except:
            pass

    # Search for refresh button in all iframes
    refresh_button = None
    
    # First try in current context - search for the SVG path directly
    print("Searching for refresh button...")
    
    # Try finding any element with the refresh SVG path
    svg_elements = driver.find_elements(By.XPATH, "//svg[contains(@d, 'M17.65')]")
    print(f"Found {len(svg_elements)} SVG elements with refresh path")
    
    for svg in svg_elements:
        try:
            # Try to find parent button
            parent = svg.find_element(By.XPATH, "./ancestor::button[1]")
            if parent.is_displayed() and parent.is_enabled():
                refresh_button = parent
                print("Found refresh button via SVG path")
                break
        except:
            pass
    
    # If not found, try searching in each iframe
    if not refresh_button:
        print("Searching in iframes...")
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for idx, iframe in enumerate(iframes):
            try:
                driver.switch_to.frame(iframe)
                time.sleep(1)
                
                # Debug: Print all buttons in this iframe
                iframe_buttons = driver.find_elements(By.TAG_NAME, "button")
                print(f"  Iframe {idx}: {len(iframe_buttons)} buttons")
                for i, btn in enumerate(iframe_buttons[:3]):
                    try:
                        print(f"    Button {i}: class={btn.get_attribute('class')}, aria-labelledby={btn.get_attribute('aria-labelledby')}")
                    except:
                        pass
                
                # Search for button with refresh SVG path
                buttons = driver.find_elements(By.XPATH, "//button[.//svg[contains(@d, 'M17.65')]]")
                for btn in buttons:
                    try:
                        if btn.is_displayed() and btn.is_enabled():
                            refresh_button = btn
                            print(f"Found refresh button in iframe {idx}")
                            break
                    except:
                        pass
                
                # Also try using aria-labelledby
                if not refresh_button:
                    buttons = driver.find_elements(By.XPATH, "//button[@aria-labelledby='page-freshness-indicator']")
                    for btn in buttons:
                        try:
                            if btn.is_displayed() and btn.is_enabled():
                                refresh_button = btn
                                print(f"Found refresh button in iframe {idx} via aria-labelledby")
                                break
                        except:
                            pass
                
                if refresh_button:
                    break
                driver.switch_to.default_content()
            except Exception as e:
                print(f"  Iframe {idx} error: {e}")
                try:
                    driver.switch_to.default_content()
                except:
                    pass
    
    if refresh_button:
        # Stay in current iframe context - don't switch away
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", refresh_button)
        driver.execute_script("arguments[0].click();", refresh_button)
        print("Clicked refresh button!")
    else:
        raise Exception("Could not find refresh button")

    print("Waiting for data to refresh...")
    time.sleep(5)

    # Use the Selenium IDE approach for download
    print("Clicking Dashboard actions button...")
    try:
        dashboard_actions = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(.,'Dashboard actions')]")
            )
        )
        dashboard_actions.click()
        print("Clicked Dashboard actions")
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
        print("Clicked Download")
        time.sleep(2)
    except Exception as e:
        print(f"Download button not found: {e}")
    
    # Wait for Format label to be visible
    print("Waiting for download dialog...")
    try:
        wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//label[contains(.,'Format')]")
            )
        )
        print("Download dialog opened")
    except Exception as e:
        print(f"Format label not found: {e}")
    
    # Wait for popup
    print("Waiting for download dialog...")

    format_box = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//input[@name='formatOption']")
        )
    )

    print("Download dialog opened")

    # ===============================
    # CSV SELECTOR (FIXED SECTION)
    # ===============================
    print("Selecting CSV...")

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

    # Click on the CSV option in the dropdown list
    try:
        csv_option = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//li[contains(.,'CSV')]")
            )
        )
        csv_option.click()
        print("Clicked CSV option from dropdown")
    except:
        # If li not found, try pressing ENTER
        format_box.send_keys(Keys.ENTER)
        print("Pressed ENTER to select CSV")

    time.sleep(1)

    # WAIT UNTIL VALUE IS REALLY CSV
    wait.until(
        lambda d: d.find_element(
            By.XPATH,
            "//input[@name='formatOption']"
        ).get_attribute("value").strip().upper() == "CSV"
    )

    print("CSV confirmed selected")
    time.sleep(2)

    # ===============================
    # CLICK CORRECT DOWNLOAD BUTTON
    # ===============================
    print("Clicking CSV Download...")

    buttons = driver.find_elements(
        By.XPATH,
        "//button[normalize-space()='Download']"
    )

    clicked = False

    for btn in reversed(buttons):
        try:
            if btn.is_displayed() and btn.is_enabled():
                driver.execute_script("arguments[0].click();", btn)
                print("Clicked CSV Download button")
                clicked = True
                break
        except:
            pass

    if not clicked:
        raise Exception("CSV Download button not found")

    print("Waiting for download to complete...")
    
    time.sleep(10)
    
    file_path = wait_for_download_and_rename(ROLLER_DOWNLOAD_PATH)
    
    # Wait a bit before returning to ensure file is released
    print("Waiting for file to be released...")
    time.sleep(5)

    driver.quit()

    return file_path
