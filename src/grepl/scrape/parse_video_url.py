# Get a timestamped video URL from a URL like this:
# https://www.youtube-nocookie.com/embed/EC_r9-TsmWY?autoplay=1&mute=1&controls=1&origin=https%3A%2F%2Foutlierdb.com&playsinline=1&showinfo=0&rel=0&iv_load_policy=3&modestbranding=1&preload=auto&enablejsapi=1&widgetid=16711&forigin=https%3A%2F%2Foutlierdb.com%2F&aoriginsup=1&vf=3
# and a caption like this:
# 01:03 - From mutual ashi, one player threatens a toe hold forcing the defender to roll to the side. They go out of bounds and reset to a neutral standing position.

"""
Get a timestamped video URL from a URL like this:
https://www.youtube-nocookie.com/embed/EC_r9-TsmWY?autoplay=1&mute=1&controls=1&origin=https%3A%2F%2Foutlierdb.com&playsinline=1&showinfo=0&rel=0&iv_load_policy=3&modestbranding=1&preload=auto&enablejsapi=1&widgetid=16711&forigin=https%3A%2F%2Foutlierdb.com%2F&aoriginsup=1&vf=3
and a caption like this:
01:03 - From mutual ashi, one player threatens a toe hold forcing the defender to roll to the side. They go out of bounds and reset to a neutral standing position.

Note: may have valid duplicate timestamped_urls (see youtube_id = 'xWkxJg8mB6w')
"""
import sqlite3
import re
from tqdm import tqdm

class VideoUrlParser:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.drop_table()
        self.create_table()

    def drop_table(self):
        with self.conn:
            self.conn.execute("DROP TABLE IF EXISTS parsed_page_enriched")

    def create_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_page_enriched (
                    timestamped_url    TEXT,
                    total_seconds      INTEGER,
                    youtube_id         TEXT,
                    caption            TEXT,
                    tags               TEXT
                )
                """
            )

    @classmethod
    def parse_timestamp_from_caption(cls, youtube_id: str, caption: str) -> tuple[int, str]:
        """Return a Youtube URL with a timestamp (inferred from the caption) added."""
        # Confirm that youtube_id is valid
        if youtube_id is None or len(youtube_id) != 11:
            raise ValueError(f"youtube_id {youtube_id} appears to be invalid")
        # Extract timestamp from caption, e.g., '01:03' or '01:23:45'
        # TODO later, try an optional hours group: ^(?:(\d{1,2}):)?(\d{1,2}):(\d{2})
        total_seconds = None
        if hour_ts_match := re.match(r'^(\d{1,2}):(\d{2}):(\d{2})', caption):
            hours, minutes, seconds = hour_ts_match.groups()
            total_seconds = (
                int(hours) * 60 * 60 +
                int(minutes) * 60 +
                int(seconds)
            )
        elif ts_match := re.match(r'^(\d{1,2}):(\d{2})', caption):
            minutes, seconds = ts_match.groups()
            total_seconds = int(minutes) * 60 + int(seconds)
        if total_seconds is not None:
            return total_seconds, f'https://www.youtube.com/watch?v={youtube_id}&t={total_seconds}s'
        raise ValueError(f"Expected to extract a timestamp from {caption} for youtube_id {youtube_id}")

    def parse_all(self):
        """Parses and saves all timestamped video URLs."""
        n_rows = self.conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT youtube_id, caption, tags FROM parsed_page)").fetchone()[0]
        for row in tqdm(self.conn.execute("SELECT DISTINCT youtube_id, caption, tags FROM parsed_page"), total=n_rows):
            youtube_id, caption, tags = row
            if youtube_id and caption and tags:
                total_seconds, timestamped_url = self.parse_timestamp_from_caption(youtube_id, caption)
                self.save_parsed_video_url(timestamped_url, total_seconds, youtube_id, caption, tags)

    def save_parsed_video_url(self, timestamped_url, total_seconds, youtube_id, caption, tags):
        """Save parsed timestamped video URL to DB."""
        with self.conn:
            self.conn.execute(
                "INSERT INTO parsed_page_enriched (timestamped_url, total_seconds, youtube_id, caption, tags) VALUES (?, ?, ?, ?, ?)",
                (timestamped_url, total_seconds, youtube_id, caption, tags)
            )

if __name__ == "__main__":
    db = VideoUrlParser()
    db.parse_all()
    