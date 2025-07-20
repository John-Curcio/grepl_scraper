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
from typing import List, Union
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


def download_clip(
    video_id: Union[str, List[str]],
    start: int,
    duration: int = 60,
    output_dir: str = 'clips',
    bw: bool = False,
    *,
    _allow_fallback: bool = True,
) -> Union[str, List[str]]:
    """
    Download a clip or clips of length `duration` seconds starting at `start`, 240p max, no audio.
    
    Args:
        video_id: A single video ID string or a list of video IDs
        start: Start time in seconds
        duration: Duration of the clip in seconds (default: 60)
        output_dir: Output directory for downloaded clips (default: 'clips')
        bw: Convert to black and white (default: False)
        _allow_fallback: Allow falling back to individual downloads if bulk download fails (default: True)
        
    Returns:
        If a single video_id was provided: path to the downloaded clip
        If a list of video_ids was provided: list of paths to the downloaded clips
    """
    is_single = isinstance(video_id, str)
    video_ids = [video_id] if is_single else video_id

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # -------------------------------------------------
    # Identify which clips still need to be downloaded
    # -------------------------------------------------
    existing_paths: dict[str, str] = {}
    pending_video_ids: list[str] = []
    for vid in video_ids:
        final_name = f"{vid}_{start}_{duration}{'_bw' if bw else ''}.mp4"
        final_path = os.path.join(output_dir, final_name)
        if os.path.exists(final_path):
            existing_paths[vid] = final_path
        else:
            pending_video_ids.append(vid)

    # If every requested clip is already present on disk, return immediately.
    if not pending_video_ids:
        return existing_paths[video_ids[0]] if is_single else [existing_paths[v] for v in video_ids]

    # -----------------------------
    # Build common yt-dlp options
    # -----------------------------
    if bw:
        # First download to a temporary colored file. We'll convert later
        outtmpl_template = os.path.join(output_dir, f"%(id)s_{start}_{duration}.temp.mp4")
    else:
        outtmpl_template = os.path.join(output_dir, f"%(id)s_{start}_{duration}.mp4")

    args_list = ['-ss', str(start), '-t', str(duration), '-an']
    ydl_opts = {
        'format': 'bestvideo[height<=240]',
        'outtmpl': outtmpl_template,
        'external_downloader': 'ffmpeg',
        'external_downloader_args': args_list,
        # Added below to make downloads fail fast instead of hanging forever
        'socket_timeout': 30,      # seconds per connection attempt
        'retries': 3,              # how many times to retry a failed fragment
        'noprogress': True,        # do not draw progress bars (less I/O)
        'quiet': True,
    }

    urls = [f'https://www.youtube.com/watch?v={vid}' for vid in pending_video_ids]

    # Download all videos in a SINGLE call – this avoids yt-dlp re-initialization overhead
    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download(urls)
    except Exception as e:
        # If the call contained multiple video IDs, try each one individually once.
        if _allow_fallback and len(pending_video_ids) > 1:
            print(f"Bulk download failed ({e}), falling back to individual downloads …")
            # Preserve successful, already-present clips
            paths_dict: dict[str, str | None] = existing_paths.copy()
            for vid in pending_video_ids:
                try:
                    paths_dict[vid] = download_clip(
                        vid,
                        start,
                        duration,
                        output_dir,
                        bw,
                        _allow_fallback=False,  # avoid infinite recursion
                    )
                except Exception as inner_e:
                    print(f"  – {vid} failed: {inner_e}")
                    paths_dict[vid] = None
            ordered_paths = [paths_dict.get(v) for v in video_ids]
            return ordered_paths[0] if is_single else ordered_paths

        # Either already attempted fallback or only a single video – propagate error
        raise

    # -----------------------------
    # Post-processing and assemble final path list
    # -----------------------------
    paths_dict: dict[str, str | None] = existing_paths.copy()

    for vid in pending_video_ids:
        base_path = outtmpl_template.replace('%(id)s', vid)
        if bw:
            bw_path = base_path.replace('.temp.mp4', '_bw.mp4')
            try:
                subprocess.run(['ffmpeg', '-y', '-i', base_path, '-vf', 'hue=s=0', bw_path], check=True)
                os.remove(base_path)
                paths_dict[vid] = bw_path
            except Exception as e:
                print(f"Error converting {vid} to BW: {e}")
                paths_dict[vid] = None
        else:
            paths_dict[vid] = base_path

    # Return results preserving the original order
    ordered_paths = [paths_dict.get(v) for v in video_ids]
    return ordered_paths[0] if is_single else ordered_paths


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
