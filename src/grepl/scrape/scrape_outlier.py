# Playwright scraper that captures 20 successive scroll states from OutlierDB
from bs4 import BeautifulSoup
import sqlite3
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService # Add this import
import os # Already imported but ensure it's available for log path
import tempfile, shutil, atexit

APPROX_CONTENT_HEIGHT = 900 # approximate height of a video content block. 
# found thru trial and error

class OutlierDbSqlite:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_page (
                    url           TEXT,
                    page_idx      INTEGER,
                    scroll_idx    INTEGER,
                    snapshot_ts   TEXT,  -- ISO8601 timestamp
                    content       TEXT,
                    UNIQUE(url, page_idx, scroll_idx, snapshot_ts)
                )
                """
            )

    def save_page(self, url: str, page_idx: int, scroll_idx: int, html: str) -> None:
        import datetime
        snapshot_ts = datetime.datetime.utcnow().isoformat()
        with self.conn:
            self.conn.execute(
                "INSERT INTO raw_page (url, page_idx, scroll_idx, snapshot_ts, content) VALUES (?,?,?,?,?)",
                (url, page_idx, scroll_idx, snapshot_ts, html),
            )


class OutlierDbScraper:
    def __init__(self, db: OutlierDbSqlite, email: str, password: str, headless: bool = True, pause_ms: int = 1200):
        self.db = db
        self.email = email
        self.password = password
        chrome_options = Options()
        
        # Isolate Chrome with a fresh temp profile
        # Store profile_dir on self for cleanup
        self.profile_dir = tempfile.mkdtemp(prefix="selenium-profile-")
        atexit.register(self._cleanup_profile_dir) # Register instance method for cleanup
        
        # Only use the isolated profile if running headless
        # For manual login (headless=False), use the default user profile
        if headless:
            chrome_options.add_argument(f"--user-data-dir={self.profile_dir}")
            chrome_options.add_argument("--headless=new")
        else:
            # When headless is False, connect to your existing Chrome on --remote-debugging-port
            print("Running with headless=False, connecting to Chrome debugger on localhost:9222")
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
        # Allow remote origins (Chrome v111+)
        chrome_options.add_argument("--remote-allow-origins=*")
        # Recommended flags to avoid environment conflicts
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Setup ChromeDriver service with verbose logging
        # Log file will be created in the current working directory
        log_path = os.path.join(os.getcwd(), "chromedriver.log")
        service = ChromeService(service_args=["--verbose"], log_output=log_path)
        
        print(f"Attempting to use profile directory: {self.profile_dir}")
        print(f"ChromeDriver log will be at: {log_path}")

        try:
            # Launch Chrome with custom profile and service
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"Error initializing WebDriver: {e}")
            print(f"Profile directory that caused failure: {self.profile_dir}")
            # Attempt to clean up the problematic profile dir immediately if driver init fails
            self._cleanup_profile_dir() 
            raise

        self.pause_sec = pause_ms / 1000
        self.wait = WebDriverWait(self.driver, 60)

    def _cleanup_profile_dir(self):
        if hasattr(self, 'profile_dir') and self.profile_dir:
            profile_dir_to_clean = self.profile_dir
            # Set self.profile_dir to None before attempting rmtree.
            # This prevents issues if _cleanup_profile_dir is called multiple times
            # (e.g., by close() and then by atexit if the first attempt failed or was slow).
            self.profile_dir = None 
            
            print(f"Cleaning up profile directory: {profile_dir_to_clean}")
            try:
                shutil.rmtree(profile_dir_to_clean)
                print(f"Successfully removed profile directory: {profile_dir_to_clean}")
            except Exception as e:
                print(f"Error removing profile directory {profile_dir_to_clean}: {e}")
        # else:
            # print("Debug: _cleanup_profile_dir called but profile_dir was None or not set.")


    # ---------- helpers -----------------------------------------------------
    def _scroll_container(self) -> None:
        """
        Scroll the virtualised list container to its bottom once.
        If the element changes in the future, tweak the CSS selector.
        """
        # First attempt: generic overflow‐auto div inside the main content area
        try:
            
            container = self.driver.find_element(By.CSS_SELECTOR, "div[style*='overflow: auto']")
            print("Found container, scrolling.")
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + (arguments[0].scrollHeight - arguments[0].scrollTop) / 2;", container)
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 2000;", container)
            self.driver.execute_script(f"arguments[0].scrollTop += {APPROX_CONTENT_HEIGHT};", container)
        except Exception:
            print("Warning: could not find container, attempting to scroll window.")
            self.driver.execute_script("window.scrollBy(0, 4000);")

        time.sleep(self.pause_sec)
        # Wait for page to load new content
        try:
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        except Exception:
            print("Warning: page load timeout, continuing.")

    def manual_login(self, login_url: str = "https://outlierdb.com/login") -> None:
        """
        Open a browser window for manual login. Waits for user confirmation.
        """
        self.driver.get(login_url)
        print("Please log in in the opened browser. Press Enter here to continue scraping...")
        input()

    # ---------- public API ---------------------------------------------------
    def scrape_pages(self, url: str, n_scrolls: int=20, n_pages: int=5) -> None:
        """
        Scrape n_pages of the given url, scrolling n_scrolls times per page. Use 
        click_next_btn to navigate to the next page.
        """
        self.driver.get(url)
        for page in range(n_pages):
            self.scrape_page(url, page, n_scrolls)
            if not self.click_next_btn():
                break

    def scrape_page(self, url: str, page_idx: int = 0, n_scrolls: int = 20) -> None:
        """
        Scrape the given url, scrolling n_scrolls times.
        """
        for scroll_idx in range(n_scrolls):
            print(f"Scrolling {scroll_idx}")
            self._scroll_container()
            html = self.driver.page_source
            self.db.save_page(url, page_idx, scroll_idx, html)
            # wait a bit
            time.sleep(self.pause_sec)

    def click_next_btn(self) -> bool:
        try:
            # scroll to the very bottom of the page
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.pause_sec)
            # wait for the last enabled green "Next" button via XPath
            next_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "(//button[contains(@class,'bg-green-500') and not(@disabled)])[last()]") )
            )
            next_btn.click()
            print("Clicked Next; waiting for page load")
            time.sleep(self.pause_sec)
            return True
        except Exception:
            print("Next button not found or not clickable; stopping pagination")
            return False

    def close(self) -> None:
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error during driver.quit(): {e}")
        # Explicitly clean up profile dir on close, atexit is a fallback
        self._cleanup_profile_dir()


if __name__ == "__main__":
    db = OutlierDbSqlite()
    # db.conn.execute("DROP TABLE IF EXISTS raw_page;")
    db.create_table()
    # Launch a visible browser for manual login
    # scraper = OutlierDbScraper(db, email="", password="", headless=True, pause_ms=1500)
    scraper = OutlierDbScraper(db, email="", password="", headless=False, pause_ms=1500)
    scraper.manual_login()
    try:
        scraper.scrape_pages("https://outlierdb.com/", n_scrolls=23, n_pages=2000)
    finally:
        scraper.close()
