# Playwright scraper that captures 20 successive scroll states from OutlierDB
from bs4 import BeautifulSoup
import sqlite3
import time
import datetime
import logging
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService  # Add this import
import os  # Already imported but ensure it's available for log path
import tempfile, shutil, atexit

APPROX_CONTENT_HEIGHT = 900 # approximate height of a video content block. 
# found thru trial and error

class OutlierDbSqlite:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.create_table()
        self.snapshot_ts = datetime.datetime.utcnow().isoformat()  # Default snapshot timestamp

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
        with self.conn:
            self.conn.execute(
                "INSERT INTO raw_page (url, page_idx, scroll_idx, snapshot_ts, content) VALUES (?,?,?,?,?)",
                (url, page_idx, scroll_idx, self.snapshot_ts, html),
            )


class OutlierDbScraper:
    def __init__(self, db: OutlierDbSqlite, headless: bool = True, pause_ms: int = 1200, start_page: int = 1):
        self.db = db
        self.headless = headless
        self.start_page = start_page
        self._pages_left_to_skip = start_page - 1  # Adjust for zero-based index
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
            # # Just launch a visible browser without remote debugging
            chrome_options.add_argument(f"--user-data-dir={self.profile_dir}")
            chrome_options.add_argument("--start-maximized")

            # When headless is False, connect to your existing Chrome on --remote-debugging-port
            # print("Running with headless=False, connecting to Chrome debugger on localhost:9223")
            # chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9223")

        # Allow remote origins (Chrome v111+)
        chrome_options.add_argument("--remote-allow-origins=*")
        # Recommended flags to avoid environment conflicts
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Setup ChromeDriver service with verbose logging
        # Log file will be created in the current working directory
        log_path = os.path.join(os.getcwd(), "chromedriver.log")
        service = ChromeService(service_args=["--verbose"], log_output=log_path)
        
        logging.info("Attempting to use profile directory: %s", self.profile_dir)
        logging.info("ChromeDriver log will be at: %s", log_path)

        try:
            # Launch Chrome with custom profile and service
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logging.error("Error initializing WebDriver: %s", e)
            logging.error("Profile directory that caused failure: %s", self.profile_dir)
            # Attempt to clean up the problematic profile dir immediately if driver init fails
            self._cleanup_profile_dir() 
            raise

        self.pause_sec = pause_ms / 1000
        self.wait = WebDriverWait(self.driver, 60)

    def __enter__(self):
        """Return self to allow use as a context manager."""
        return self

    def __exit__(self, exc_type, exc, tb):
        """Ensure the driver is closed when exiting a context."""
        self.close()
        # Do not suppress exceptions
        return False

    def _cleanup_profile_dir(self):
        if hasattr(self, 'profile_dir') and self.profile_dir:
            profile_dir_to_clean = self.profile_dir
            # Set self.profile_dir to None before attempting rmtree.
            # This prevents issues if _cleanup_profile_dir is called multiple times
            # (e.g., by close() and then by atexit if the first attempt failed or was slow).
            self.profile_dir = None 
            
            logging.info("Cleaning up profile directory: %s", profile_dir_to_clean)
            try:
                shutil.rmtree(profile_dir_to_clean)
                logging.info("Successfully removed profile directory: %s", profile_dir_to_clean)
            except Exception as e:
                logging.error("Error removing profile directory %s: %s", profile_dir_to_clean, e)
        # else:
            # print("Debug: _cleanup_profile_dir called but profile_dir was None or not set.")


    # ---------- helpers -----------------------------------------------------
    def _scroll_container(self, content_height: int | None = None) -> None:
        """
        Scroll the virtualised list container to its bottom once.
        If the element changes in the future, tweak the CSS selector.
        """
        if content_height is None:
            content_height = APPROX_CONTENT_HEIGHT
        # First attempt: generic overflow‐auto div inside the main content area
        try:
            
            container = self.driver.find_element(By.CSS_SELECTOR, "div[style*='overflow: auto']")
            # print("Found container, scrolling.")
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + (arguments[0].scrollHeight - arguments[0].scrollTop) / 2;", container)
            # self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 2000;", container)
            self.driver.execute_script(f"arguments[0].scrollTop += {content_height};", container)
        except Exception:
            logging.warning("Could not find container, attempting to scroll window.")
            self.driver.execute_script("window.scrollBy(0, 4000);")

        # Wait for page to load new content
        try:
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        except Exception:
            logging.warning("Page load timeout, continuing.")

    def manual_login(self, login_url: str = "https://outlierdb.com/login") -> None:
        """
        Open a browser window for manual login. Waits for user confirmation.
        """
        self.driver.get(login_url)
        logging.info("Please log in in the opened browser. Press Enter here to continue scraping...")
        input()

    def _wait_for_youtube_iframes(self, timeout=10) -> bool:
        """
        Wait for YouTube iframe elements to load on the page.
        Returns True if iframes were found, False if timeout occurred.
        """
        try:
            # Wait for iframes with YouTube URLs
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='youtube-nocookie.com/embed']"))
            )
            # print("YouTube iframes loaded successfully")
            return True
        except Exception as e:
            logging.warning("Timeout waiting for YouTube iframes: %s", e)
            return False

    # ---------- public API ---------------------------------------------------
    def scrape_pages(self, url: str, n_scrolls: int=20, n_pages: int=5) -> None:
        """
        Scrape n_pages of the given url, scrolling n_scrolls times per page. Use 
        click_next_btn to navigate to the next page.
        """
        for page in range(n_pages):
            if self._pages_left_to_skip > 0:
                self.skip_page(page, n_scrolls)
            else:
                self.scrape_page(url, page, n_scrolls)
                if not self.click_next_btn():
                    break

    # Add this new method for fast scrolling
    def _fast_scroll_to_bottom(self, max_scrolls=20, scroll_height=1000):
        """
        Quickly scroll to the bottom of the page to reveal the Next button
        without waiting for content to fully load.
        """
        raise NotImplementedError("Fast scrolling not implemented yet.")

    def skip_page(self, page_idx: int, n_scrolls: int) -> None:
        """
        Skip a page by quickly scrolling to the bottom to reveal the Next button.
        """
        logging.info("Fast skipping page %s", page_idx + 1)
        
        # # Use the fast scrolling method instead of regular scrolling
        for scroll_idx in tqdm(range(n_scrolls), total=n_scrolls, desc=f"Scrolling thru page {page_idx+1}"):
            # print(f"Scrolling {scroll_idx}")
            self._scroll_container()
        
            # Wait for YouTube iframes to load
            # time.sleep(self.pause_sec)
            self._wait_for_youtube_iframes()
        
        # One final scroll to ensure we're at the bottom
        try:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except Exception as e:
            logging.error("Error during final scroll: %s", e)
        
        # next button should be visible now
        if not self.click_next_btn():
            logging.warning("Next button not found after fast scroll. Manual intervention may be needed.")
            return
        # Decrement the pages left to skip
        self._pages_left_to_skip -= 1
        logging.info("Skipped page %s, remaining pages to skip: %s", page_idx + 1, self._pages_left_to_skip)

    def scrape_page(self, url: str, page_idx: int = 0, n_scrolls: int = 20) -> None:
        """
        Scrape the given url, scrolling n_scrolls times.
        """
        for scroll_idx in tqdm(range(n_scrolls), total=n_scrolls, desc=f"Scraping page {page_idx+1}"):
            # print(f"Scrolling {scroll_idx}")
            self._scroll_container()
            
            # Wait for YouTube iframes to load
            # time.sleep(self.pause_sec)
            self._wait_for_youtube_iframes()
            
            # Get page source after iframes have loaded
            html = self.driver.page_source
            self.db.save_page(url, page_idx, scroll_idx, html)


    def click_next_btn(self) -> bool:
        """
        Click the "Next" button to navigate to the next page.
        If the button is not found, try and find it. After 20 attempts, ask for manual intervention.
        """
        max_attempts = 20 # this is really high but idgaf
        
        for attempt in range(max_attempts):
            try:
                # scroll to the very bottom of the page
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                # Try a different scroll if not first attempt
                if attempt > 0:
                    logging.info(
                        "Attempt %s/%s: Trying additional scrolling...",
                        attempt + 1,
                        max_attempts,
                    )
                    # Scroll up slightly and then down again (sometimes helps reveal buttons)
                    self.driver.execute_script("window.scrollBy(0, -500);")
                    time.sleep(self.pause_sec / 2)
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # wait for the last enabled green "Next" button via XPath
                next_btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "(//button[contains(@class,'bg-green-500') and not(@disabled)])[last()]"))
                )
                next_btn.click()
                logging.info("Clicked Next; waiting for page load")
                time.sleep(self.pause_sec)
                return True
            
            except Exception as e:
                logging.warning(
                    "Attempt %s/%s failed: %s...",
                    attempt + 1,
                    max_attempts,
                    str(e)[:100],
                )
                
                # Wait a bit longer on retries
                if attempt < max_attempts - 1:
                    time.sleep(self.pause_sec * 2)
        
        # If all automated attempts failed, ask for manual intervention
        if not self.driver.execute_script("return document.hidden"):
            logging.warning("\n" + "=" * 80)
            logging.warning("MANUAL INTERVENTION NEEDED: Next button could not be found automatically.")
            logging.warning("Please navigate to the next page manually in the browser window.")
            logging.warning("After navigating to the next page, press Enter to continue scraping...")
            logging.warning("=" * 80 + "\n")
            input()  # Wait for user to confirm they've navigated

            # Return True since the user has presumably navigated to the next page
            logging.info("Resuming automated scraping...")
            return True
        else:
            logging.warning(
                "Browser is not visible (headless mode). Cannot request manual intervention."
            )
            logging.info("Stopping pagination.")
            return False

    def close(self) -> None:
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logging.error("Error during driver.quit(): %s", e)
        # Explicitly clean up profile dir on close, atexit is a fallback
        self._cleanup_profile_dir()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = OutlierDbSqlite()
    db.create_table()
    # Launch a visible browser for manual login
    with OutlierDbScraper(db, headless=False, pause_ms=1500, start_page=1) as scraper:
        scraper.manual_login()  # comment this out if we're already logged in and at the right page
        scraper.scrape_pages("https://outlierdb.com/", n_scrolls=23, n_pages=2000)
