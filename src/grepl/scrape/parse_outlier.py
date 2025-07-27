"""
TODO items:
* can we refactor this into another dataclass?
* don't like this extra for loop in parse_all
* get rid of url, page_idx, scroll_idx? maybe just snapshot_ts
* If I get multiple youtube_ids in a block, i want to crash. is that actually working here?
"""

import bs4
import sqlite3
import re
from tqdm import tqdm

class ParsedOutlierDbSqlite:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.drop_table()
        self.create_table()

    @staticmethod
    def _extract_youtube_id_from_block(block: bs4.element.Tag) -> str:
        """
        Extracts the YouTube video ID from a block of HTML content.

        If multiple YouTube IDs are present, throw an error - the block should only contain one YouTube video!

        Look for the following patterns:
        * src="https://www.youtube-nocookie.com/embed/HKbj5Zljmwo?autoplay=1&amp;mute=1&amp;controls=1&amp;origin=https%3A%2F%2Foutlierdb.com&amp;playsinline=1&amp;showinfo=0&amp;rel=0&amp;iv_load_policy=3&amp;modestbranding=1&amp;preload=auto&amp;enablejsapi=1&amp;widgetid=24313&amp;forigin=https%3A%2F%2Foutlierdb.com%2F&amp;aoriginsup=1&amp;vf=6"
        * src="https://img.youtube.com/vi/XgD5w6jW8Io/hqdefault.jpg"

        Args:
            block: A BeautifulSoup object representing a block of HTML content.

        Returns:
            str: The YouTube video ID if found, otherwise an empty string.

        """
        youtube_ids = set()

        # Find all <iframe> tags with YouTube URLs
        for iframe in block.find_all('iframe'):
            src = iframe.get('src', '')
            match = re.search(r'youtube-nocookie\.com/embed/([a-zA-Z0-9_-]+)', src)
            if match:
                youtube_ids.add(match.group(1))

        # Find all <img> tags with YouTube thumbnail URLs
        for img in block.find_all('img'):
            src = img.get('src', '')
            match = re.search(r'img\.youtube\.com/vi/([a-zA-Z0-9_-]+)/hqdefault\.jpg', src)
            if match:
                youtube_ids.add(match.group(1))

        if len(youtube_ids) == 1:
            return youtube_ids.pop()
        elif len(youtube_ids) > 1:
            raise ValueError(f"Multiple YouTube IDs found in block: {youtube_ids}")
        return ''

    @staticmethod
    def _extract_description_from_block(block: bs4.element.Tag) -> str:
        """
        Extracts the video description from a block of HTML content.

        Args:
            block: A BeautifulSoup object representing a block of HTML content.

        Returns:
            str: The video description if found, otherwise an empty string.
        """
        # example block:
        # <p class="text-neutral-900 dark:text-neutral-100 my-4 p-2">00:34 - A single leg takedown.</p>
        video_description = block.find('p', class_='text-neutral-900 dark:text-neutral-100 my-4 p-2')
        if not video_description:
            raise ValueError("No video description found in block")
        return video_description.text.strip()

    @staticmethod
    def _extract_tags_from_block(block: bs4.element.Tag) -> list[str]:
        """
        Extracts the tags from a block of HTML content.

        Args:
            block: A BeautifulSoup object representing a block of HTML content.

        Returns:
            list[str]: A list of tags found in the block.
        """
        # example content:
        # <span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  
        # bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#wrestling</span><span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#takedown</span><span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#standing</span><span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#singleleg</span><span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#singlelegtakedown</span><span class="py-2 px-3 border border-neutral-400 dark:border-neutral-500 cursor-pointer  bg-gray-200 dark:bg-neutral-600 text-gray-800 dark:text-gray-200 text-xs rounded-md hover:bg-gray-300 dark:hover:bg-neutral-500 hover:text-neutral-900 dark:hover:text-neutral-100 hover:dark:border-neutral-300">#footage</span>
        tags = []
        for span in block.find_all('span'):
            tag = span.text.strip()
            if tag and tag.startswith('#'):
                # Ensure the tag is unique and not empty
                tags.append(tag)
        return tags

    @staticmethod
    def _extract_data_from_block(block: bs4.element.Tag) -> dict:
        """
        Extracts data from a single block of HTML content.

        Args:
            block: A BeautifulSoup object representing a block of HTML content.

        Returns:
            dict: A dictionary containing the extracted data.
        """
        try:
            youtube_id = ParsedOutlierDbSqlite._extract_youtube_id_from_block(block)
            video_description = ParsedOutlierDbSqlite._extract_description_from_block(block)
            tags = ParsedOutlierDbSqlite._extract_tags_from_block(block)
        except ValueError as e:
            print(f"Error extracting data: {e}")
            print(block.contents)
            raise ValueError(f"Failed to extract data from block: {block}") from e

        return {
            'youtube_id': youtube_id,
            'caption': video_description,
            'tags': tags,
        }

    # ---- public api -----

    def create_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_page (
                    youtube_id TEXT,
                    caption TEXT,
                    tags TEXT,
                    -- use to link back to raw_page
                    url           TEXT,
                    page_idx      INTEGER,
                    scroll_idx    INTEGER,
                    snapshot_ts   TEXT
                )
                """
            )

    def drop_table(self):
        with self.conn:
            self.conn.execute("DROP TABLE IF EXISTS parsed_page")


    def save_parsed_page(self, url: str, page_idx: int, scroll_idx: int, tags: list[str], 
                        youtube_id: str, caption: str, snapshot_ts: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO parsed_page (url, page_idx, scroll_idx, snapshot_ts, tags, youtube_id, caption) VALUES (?,?,?,?,?,?,?)",
                (url, page_idx, scroll_idx, snapshot_ts, ",".join(tags), youtube_id, caption),
            )

    @classmethod
    def extract_data_from_html(cls, html_content: str) -> list[dict]:
        """
        Extracts relevant data from the provided HTML content.

        The HTML content might contain multiple blocks of data, each containing:
        * Video URL
        * Video description (including a timestamp)
        * Tags (eg #footage, #wrestling, #singleleg)

        This function
        * parses the HTML content into blocks
        * extracts the aforementioned data per block
        * returns a list of dictionaries with the extracted data

        Args:
            html_content (str): The HTML content to extract data from.

        Returns:
            list[dict]: A list of dictionaries, each containing the extracted data from a block.
        """

        soup = bs4.BeautifulSoup(html_content, 'html.parser')
        data = []

        # Find all relevant blocks in the HTML content
        for block in soup.find_all('div', class_='flex justify-center sequence-card'):
            data.append(cls._extract_data_from_block(block))

        return data

    def parse_all(self, min_snapshot_ts:str|None = None):
        """Parses and saves all pages in the raw_page table."""
        if min_snapshot_ts is None:
            min_snapshot_ts = '1900-01-01' # something absurdly low
        n_rows_query = f"SELECT COUNT(*) FROM raw_page WHERE snapshot_ts >= {min_snapshot_ts}"
        n_rows = self.conn.execute(n_rows_query).fetchone()[0]

        all_rows_query = f"SELECT * FROM raw_page WHERE snapshot_ts >= {min_snapshot_ts}"
        
        # final_data = []
        for row in tqdm(self.conn.execute(all_rows_query), total=n_rows):
            url, page_idx, scroll_idx, snapshot_ts, content = row
            curr_data = ParsedOutlierDbSqlite.extract_data_from_html(content)
            # TODO don't like this for loop... ugh
            for entry in curr_data:
                tags = entry["tags"]
                youtube_id = entry["youtube_id"]
                caption = entry["caption"]
                self.save_parsed_page(url, page_idx, scroll_idx, tags, youtube_id, caption, snapshot_ts)
            # TODO
            # curr_data = pd.DataFrame(curr_data)
            # curr_data["url"] = url
            # curr_data["page_idx"] = page_idx
            # curr_data["scroll_idx"] = scroll_idx
            # curr_data["snapshot_ts"] = snapshot_ts
            # final_data.append(curr_data)
            
        
if __name__ == "__main__":
    db = ParsedOutlierDbSqlite()
    # db.drop_table()
    db.parse_all()
    db.parse_all()
