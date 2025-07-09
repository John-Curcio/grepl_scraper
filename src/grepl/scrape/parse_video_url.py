# Get a timestamped video URL from a URL like this:
# https://www.youtube-nocookie.com/embed/EC_r9-TsmWY?autoplay=1&mute=1&controls=1&origin=https%3A%2F%2Foutlierdb.com&playsinline=1&showinfo=0&rel=0&iv_load_policy=3&modestbranding=1&preload=auto&enablejsapi=1&widgetid=16711&forigin=https%3A%2F%2Foutlierdb.com%2F&aoriginsup=1&vf=3
# and a caption like this:
# 01:03 - From mutual ashi, one player threatens a toe hold forcing the defender to roll to the side. They go out of bounds and reset to a neutral standing position.

import sqlite3
import re
from tqdm import tqdm

class VideoUrlParser:
    conn = sqlite3.connect("outlierdb.sqlite")

    def __init__(self):
        self.create_table()

    def create_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS parsed_video_url (
                    video_url     TEXT,
                    caption       TEXT,
                    timestamped_url TEXT,
                    UNIQUE(video_url, caption)
                )
                """
            )

    def _parse_video_url(self, video_url: str, caption: str) -> str:
        # Return a video URL with a timestamp added
        # if video_url doesn't match the pattern, return None
        # Extract YouTube video ID from embed or watch URL
        vid_match = re.search(r'/embed/([^?]+)', video_url)
        if vid_match:
            video_id = vid_match.group(1)
        else:
            vid_match = re.search(r'v=([^&]+)', video_url)
            if vid_match:
                video_id = vid_match.group(1)
            else:
                return None
        # Extract timestamp from caption, e.g., '01:03'
        ts_match = re.match(r'^(\d{1,2}):(\d{2})', caption)
        if ts_match:
            minutes, seconds = ts_match.groups()
            total_seconds = int(minutes) * 60 + int(seconds)
            return f'https://www.youtube.com/watch?v={video_id}&t={total_seconds}s'
        # No timestamp found; return standard watch URL
        return f'https://www.youtube.com/watch?v={video_id}'

    def parse_all(self):
        """Parses and saves all timestamped video URLs."""
        # for row in self.conn.execute("SELECT video_url, caption FROM parsed_page"):
        n_rows = self.conn.execute("SELECT COUNT(*) FROM parsed_page").fetchone()[0]
        for row in tqdm(self.conn.execute("SELECT video_url, caption FROM parsed_page where video_url not null and video_url != ''"), total=n_rows):
            video_url, caption = row
            timestamped_url = self._parse_video_url(video_url, caption)
            self.save_parsed_video_url(video_url, caption, timestamped_url)

    def save_parsed_video_url(self, video_url: str, caption: str, timestamped_url: str):
        """Save parsed timestamped video URL to DB."""
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO parsed_video_url (video_url, caption, timestamped_url) VALUES (?, ?, ?)",
                (video_url, caption, timestamped_url),
            )

if __name__ == "__main__":
    db = VideoUrlParser()
    db.parse_all()
    