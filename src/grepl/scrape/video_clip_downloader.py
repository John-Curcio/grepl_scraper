"""
Module to download 60-second, 240p (or lower), no-audio clips from YouTube timestamped URLs.
Dependencies: yt-dlp (pip install yt-dlp), ffmpeg must be in PATH.

TODO untested!

# https://www.youtube.com/watch?v=VKpxTsdnPiI&t=121s

Example:

poetry run python src/grepl/scrape/video_clip_downloader.py \
    https://www.youtube.com/watch?v=VKpxTsdnPiI&t=121s
"""
import os
import subprocess
import re
from urllib.parse import urlparse, parse_qs
from yt_dlp import YoutubeDL

def parse_timestamped_url(url: str) -> tuple[str, int]:
    """
    Extract video ID and start time (in seconds) from a timestamped YouTube URL.
    Supports URLs like:
      https://www.youtube.com/watch?v=VIDEO_ID&t=63s
      https://www.youtube.com/watch?v=VIDEO_ID&other=params&t=1m3s
      https://youtu.be/Szj2-YS3J2o\?t\=115 TODO
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    # video ID
    video_id = None
    # support short youtu.be URLs
    if parsed.netloc.endswith("youtu.be"):
        video_id = parsed.path.lstrip("/")
    elif "v" in qs:
        video_id = qs["v"][0]
    else:
        m = re.search(r'/embed/([^&?/]+)', url)
        if m:
            video_id = m.group(1)
    if not video_id:
        raise ValueError(f"Cannot parse video ID from URL: {url}")
    # timestamp
    t = qs.get("t", [None])[0]
    start = 0
    if t:
        m = re.match(r'(?:(\d+)m)?(\d+)s?', t)
        if m:
            mins = m.group(1)
            secs = m.group(2)
            start = int(secs)
            if mins:
                start += int(mins) * 60
        else:
            # fallback numeric seconds
            start = int(t.rstrip('s'))
    return video_id, start


def download_clip(video_id: str, start: int, duration: int = 60, output_dir: str = 'clips', bw: bool = False) -> str:
    """
    Download a clip of length `duration` seconds starting at `start`, 240p max, no audio.
    Returns the output file path.
    """
    os.makedirs(output_dir, exist_ok=True)
    if bw:
        filename = f"{video_id}_{start}.temp.mp4"
    else:
        filename = f"{video_id}_{start}.mp4"
    outtmpl = os.path.join(output_dir, filename)
    args_list = ['-ss', str(start), '-t', str(duration), '-an']
    ydl_opts = {
        'format': 'bestvideo[height<=240]',
        'outtmpl': outtmpl,
        'external_downloader': 'ffmpeg',
        'external_downloader_args': args_list,
        'quiet': True,
    }
    url = f'https://www.youtube.com/watch?v={video_id}'
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
        # If black-and-white requested, convert after download
    if bw:
        raw_path = outtmpl
        bw_filename = f"{video_id}_{start}_bw.mp4"
        bw_path = os.path.join(output_dir, bw_filename)
        subprocess.run(['ffmpeg', '-y', '-i', raw_path, '-vf', 'hue=s=0', bw_path], check=True)
        os.remove(raw_path)
        return bw_path
    return outtmpl


def download_clip_from_timestamped_url(ts_url: str, duration: int = 60, output_dir: str = 'clips', bw: bool = False) -> str:
    """
    Convenience: parse timestamped URL and download clip.
    """
    vid, start = parse_timestamped_url(ts_url)
    return download_clip(vid, start, duration, output_dir, bw)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Download 60s clip from a timestamped YouTube URL.')
    parser.add_argument('url', help='Timestamped YouTube URL (with &t=xxs)')
    parser.add_argument('--duration', '-d', type=int, default=60, help='Clip length in seconds')
    parser.add_argument('--output-dir', '-o', default='clips', help='Output directory')
    parser.add_argument('--bw', action='store_true', help='Download clip in black and white')
    args = parser.parse_args()
    clip_path = download_clip_from_timestamped_url(args.url, args.duration, args.output_dir, args.bw)
    print(f"Downloaded clip to: {clip_path}")

