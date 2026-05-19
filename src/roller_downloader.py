import os
import shutil
import time

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from config.config import (
    PAGE_LOAD_TIMEOUT,
    ROLLER_DASHBOARD_URL,
    ROLLER_DOWNLOAD_PATH,
    ROLLER_DOWNLOAD_TIMEOUT,
    ROLLER_LOGIN_URL,
    ROLLER_PASSWORD,
    ROLLER_REPORT_NAME,
    ROLLER_RETRIES,
    ROLLER_USERNAME,
    SCRIPT_TIMEOUT,
)


def start_driver():
    chrome_options = Options()
    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": os.path.abspath(ROLLER_DOWNLOAD_PATH),
            "download.prompt_for_download": False,
        },
    )

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
        except WebDriverException as exc:
            print(f"{label} failed to load: {exc}")

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
    except TimeoutException as exc:
        raise TimeoutException(f"Timed out waiting for {description}") from exc


def wait_for_download_and_rename(download_path, timeout=ROLLER_DOWNLOAD_TIMEOUT):
    print("Waiting for download...")
    start_time = time.time()

    while True:
        files = [
            file_name
            for file_name in os.listdir(download_path)
            if not file_name.endswith(".crdownload")
            and os.path.isfile(os.path.join(download_path, file_name))
        ]

        if files:
            latest = max(
                [os.path.join(download_path, file_name) for file_name in files],
                key=os.path.getctime,
            )
            ext = os.path.splitext(latest)[1] or ".csv"
            final_path = os.path.join(download_path, f"roller_data{ext}")

            if os.path.abspath(latest) == os.path.abspath(final_path):
                print("Saved:", final_path)
                return final_path

            if os.path.exists(final_path):
                try:
                    os.remove(final_path)
                except OSError:
                    pass

            try:
                os.rename(latest, final_path)
            except OSError:
                shutil.copy2(latest, final_path)
                os.remove(latest)

            print("Saved:", final_path)
            return final_path

        if time.time() - start_time > timeout:
            raise TimeoutException(f"Download timeout after {timeout} seconds")

        time.sleep(2)


def find_refresh_button(driver):
    refresh_button = _find_refresh_button_in_current_frame(driver)
    if refresh_button:
        return refresh_button

    print("Searching in iframes...")
    for idx, iframe in enumerate(driver.find_elements(By.TAG_NAME, "iframe")):
        try:
            driver.switch_to.frame(iframe)
            time.sleep(1)
            refresh_button = _find_refresh_button_in_current_frame(driver, idx)
            if refresh_button:
                return refresh_button
            driver.switch_to.default_content()
        except WebDriverException as exc:
            print(f"  Iframe {idx} error: {exc}")
            try:
                driver.switch_to.default_content()
            except WebDriverException:
                pass

    return None


def _find_refresh_button_in_current_frame(driver, iframe_index=None):
    if iframe_index is None:
        print("Searching for refresh button...")
        svg_elements = driver.find_elements(By.XPATH, "//svg[contains(@d, 'M17.65')]")
        print(f"Found {len(svg_elements)} SVG elements with refresh path")
        for svg in svg_elements:
            try:
                parent = svg.find_element(By.XPATH, "./ancestor::button[1]")
                if parent.is_displayed() and parent.is_enabled():
                    print("Found refresh button via SVG path")
                    return parent
            except WebDriverException:
                pass
    else:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        print(f"  Iframe {iframe_index}: {len(buttons)} buttons")

    locators = [
        (By.XPATH, "//button[.//svg[contains(@d, 'M17.65')]]"),
        (By.XPATH, "//button[@aria-labelledby='page-freshness-indicator']"),
    ]

    for locator in locators:
        for button in driver.find_elements(*locator):
            try:
                if button.is_displayed() and button.is_enabled():
                    print("Found refresh button")
                    return button
            except WebDriverException:
                pass

    return None


def click_csv_download(driver):
    print("Clicking Dashboard actions button...")
    try:
        dashboard_actions = wait_for(
            driver,
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(.,'Dashboard actions')]")
            ),
            "Dashboard actions button",
        )
        dashboard_actions.click()
        print("Clicked Dashboard actions")
        time.sleep(1)
    except TimeoutException as exc:
        print(f"Dashboard actions not found: {exc}")

    print("Clicking Download...")
    try:
        download_btn = wait_for(
            driver,
            EC.element_to_be_clickable((By.XPATH, "//span[contains(.,'Download')]")),
            "Download menu item",
        )
        download_btn.click()
        print("Clicked Download")
        time.sleep(2)
    except TimeoutException as exc:
        print(f"Download button not found: {exc}")

    print("Waiting for download dialog...")
    wait_for(
        driver,
        EC.visibility_of_element_located((By.XPATH, "//label[contains(.,'Format')]")),
        "download dialog Format label",
    )
    print("Download dialog opened")

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
        print("Clicked CSV option from dropdown")
    except TimeoutException:
        format_box.send_keys(Keys.ENTER)
        print("Pressed ENTER to select CSV")

    wait_for(
        driver,
        lambda d: d.find_element(
            By.XPATH,
            "//input[@name='formatOption']",
        ).get_attribute("value").strip().upper() == "CSV",
        "CSV selection",
    )
    print("CSV confirmed selected")
    time.sleep(2)

    print("Clicking CSV Download...")
    buttons = driver.find_elements(By.XPATH, "//button[normalize-space()='Download']")
    for button in reversed(buttons):
        try:
            if button.is_displayed() and button.is_enabled():
                driver.execute_script("arguments[0].click();", button)
                print("Clicked CSV Download button")
                return
        except WebDriverException:
            pass

    raise TimeoutException("CSV Download button not found")


def _download_dashboard_once():
    driver = start_driver()

    try:
        safe_get(driver, ROLLER_LOGIN_URL, "login page")

        wait_for(
            driver,
            EC.presence_of_element_located((By.ID, "username")),
            "login username field",
        ).send_keys(ROLLER_USERNAME)
        driver.find_element(By.ID, "password").send_keys(ROLLER_PASSWORD)
        driver.find_element(By.NAME, "action").click()
        time.sleep(6)

        safe_get(driver, ROLLER_DASHBOARD_URL, "dashboard page")
        time.sleep(8)

        wait_for(
            driver,
            EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe")),
            "dashboard iframe",
        )

        dashboard = wait_for(
            driver,
            EC.element_to_be_clickable(
                (By.XPATH, f"//*[contains(text(),'{ROLLER_REPORT_NAME}')]")
            ),
            f"{ROLLER_REPORT_NAME} dashboard",
        )
        dashboard.click()
        time.sleep(10)

        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        buttons = driver.find_elements(By.TAG_NAME, "button")
        print(f"Found {len(iframes)} iframes")
        print(f"Found {len(buttons)} buttons on page")

        refresh_button = find_refresh_button(driver)
        if not refresh_button:
            raise TimeoutException("Could not find refresh button")

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", refresh_button)
        driver.execute_script("arguments[0].click();", refresh_button)
        print("Clicked refresh button!")

        print("Waiting for data to refresh...")
        time.sleep(5)

        click_csv_download(driver)

        print("Waiting for download to complete...")
        time.sleep(10)
        file_path = wait_for_download_and_rename(ROLLER_DOWNLOAD_PATH)

        print("Waiting for file to be released...")
        time.sleep(5)
        return file_path
    finally:
        try:
            driver.quit()
        except WebDriverException:
            pass


def download_dashboard(retries=ROLLER_RETRIES):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"Starting Roller download attempt {attempt}/{retries}...")
            return _download_dashboard_once()
        except Exception as exc:
            last_error = exc
            print(f"Roller download attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                print("Restarting browser and trying Roller again...")
                time.sleep(10)

    raise RuntimeError(f"Roller download failed after {retries} attempts") from last_error
