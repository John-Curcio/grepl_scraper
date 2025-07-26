# TODO: parse results of raw_page into
# - tags
# - video URL (hopefully including the timestamp)
# - title
# - caption

from bs4 import BeautifulSoup
import sqlite3
import re
from tqdm import tqdm

class ParsedOutlierDbSqlite:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_page (
                    url           TEXT,
                    page_idx      INTEGER,
                    scroll_idx    INTEGER,
                    snapshot_ts   TEXT,  -- Added snapshot_ts field
                    tags          TEXT,
                    video_url     TEXT,
                    title         TEXT,
                    caption       TEXT,
                    UNIQUE(url, page_idx, scroll_idx, snapshot_ts)
                )
                """
            )

    def drop_table(self):
        with self.conn:
            self.conn.execute("DROP TABLE IF EXISTS parsed_page")

    def parse_page(self, url: str, page_idx: int, scroll_idx: int) -> (list[str], str, str, str, str):
        """Reads the raw_page content for the given url, page_idx, scroll_idx,
        and parses it into tags, video_url, title, and caption."""
        with self.conn:
            raw_page = self.conn.execute(
                "SELECT content, snapshot_ts FROM raw_page WHERE url = ? AND page_idx = ? AND scroll_idx = ?",
                (url, page_idx, scroll_idx),
            ).fetchone()
            if not raw_page:
                raise ValueError(f"No raw_page found for {url}, {page_idx}, {scroll_idx}")
            content, snapshot_ts = raw_page
            soup = BeautifulSoup(content, "html.parser")
            tags = self._parse_tags(soup)
            video_url = self._parse_video_url(soup)
            title = self._parse_title(soup)
            caption = self._parse_caption(soup)
        return tags, video_url, title, caption, snapshot_ts

    def _parse_tags(self, soup: BeautifulSoup) -> list[str]:
        # find the most recent sequence card
        cards = soup.find_all("div", class_="sequence-card")
        if not cards:
            return []
        card = cards[-1]
        # extract tag spans
        tag_spans = card.find_all("span")
        return [span.text.strip() for span in tag_spans] 

    def _parse_video_url(self, soup: BeautifulSoup) -> str:
        # find the most recent sequence card
        cards = soup.find_all("div", class_="sequence-card")
        if not cards:
            return ""
        card = cards[-1]
        # extract iframe src
        iframe = card.find("iframe")
        return iframe.get("src", "") if iframe else "" 

    def _parse_title(self, soup: BeautifulSoup) -> str:
        # use the page title
        title_tag = soup.find("title")
        return title_tag.text.strip() if title_tag else "" 

    def _parse_caption(self, soup: BeautifulSoup) -> str:
        # find the most recent sequence card
        cards = soup.find_all("div", class_="sequence-card")
        if not cards:
            return ""
        card = cards[-1]
        # extract caption paragraph
        p = card.find("p")
        return p.text.strip() if p else "" 

    def save_parsed_page(self, url: str, page_idx: int, scroll_idx: int, tags: list[str], 
                        video_url: str, title: str, caption: str, snapshot_ts: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO parsed_page (url, page_idx, scroll_idx, snapshot_ts, tags, video_url, title, caption) VALUES (?,?,?,?,?,?,?,?)",
                (url, page_idx, scroll_idx, snapshot_ts, ",".join(tags), video_url, title, caption),
            )

    def parse_all(self):
        """Parses and saves all pages in the raw_page table."""
        n_rows = self.conn.execute("SELECT COUNT(*) FROM raw_page").fetchone()[0]
        for row in tqdm(self.conn.execute("SELECT url, page_idx, scroll_idx FROM raw_page"), total=n_rows):
            url, page_idx, scroll_idx = row
            tags, video_url, title, caption, snapshot_ts = self.parse_page(url, page_idx, scroll_idx)
            self.save_parsed_page(url, page_idx, scroll_idx, tags, video_url, title, caption, snapshot_ts)
            
        
if __name__ == "__main__":
    db = ParsedOutlierDbSqlite()
    db.parse_all()
