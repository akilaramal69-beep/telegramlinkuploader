import asyncio
import time
import os
import json
import mimetypes
import re
import shutil
import urllib.parse
import aiohttp
import aiofiles
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aria2p
from plugins.config import Config
from utils.shared import WEBAPP_PROGRESS, get_http_session

PROGRESS_UPDATE_DELAY = 1  # seconds between progress edits

# Global dictionary used by the Flask Mini App to read live progress percentages
# Replaced by Config.WEBAPP_PROGRESS for thread-safe singleton access



def _get_ffmpeg_bin() -> str:
    """Return the actual ffmpeg binary path, checking FFMPEG_PATH and PATH."""
    path = Config.FFMPEG_PATH  # could be 'ffmpeg' or '/usr/bin/ffmpeg'
    # If it already looks like a binary (has no dir separators), use shutil.which
    if os.sep not in path and '/' not in path:
        found = shutil.which(path)
        if found:
            return found
    if os.path.isfile(path):
        return path
    # Last resort: try well-known locations
    for candidate in ["/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
        if os.path.isfile(candidate):
            return candidate
    return path  # return whatever was configured, let the caller fail


def _get_ffmpeg_dir() -> str:
    """Return the DIRECTORY containing ffmpeg — what yt-dlp expects for ffmpeg_location."""
    return os.path.dirname(_get_ffmpeg_bin()) or None


def _get_ffprobe_bin() -> str:
    """Return the ffprobe binary path (same dir as ffmpeg)."""
    ffmpeg_dir = os.path.dirname(_get_ffmpeg_bin())
    ffprobe = os.path.join(ffmpeg_dir, "ffprobe") if ffmpeg_dir else "ffprobe"
    if os.path.isfile(ffprobe):
        return ffprobe
    found = shutil.which("ffprobe")
    return found or "ffprobe"

# ── Streaming / HLS detection ─────────────────────────────────────────────────

# Extensions that indicate a playlist / stream, not a direct media file
STREAMING_EXTENSIONS: dict[str, str] = {
    ".m3u8": ".mp4",
    ".m3u":  ".mp4",
    ".mpd":  ".mp4",   # DASH manifest
    ".ts":   ".mp4",   # raw MPEG-TS segment
}

HLS_MIME_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "application/dash+xml",
    "audio/mpegurl",
    "audio/x-mpegurl",
    "video/mp2t",
}


def needs_ffmpeg_download(url: str, mime: str) -> bool:
    """Return True if this URL must be downloaded with ffmpeg instead of aiohttp."""
    ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()
    return ext in STREAMING_EXTENSIONS or (mime or "").lower() in HLS_MIME_TYPES

async def resolve_url(url: str) -> str:
    """Resolve redirecting URLs (like reddit shortlinks) and bypass Twitter NSFW blocks."""
    # 1. Resolve Reddit short links
    if "redd.it" in url or "/s/" in url.lower():
        for _ in range(2): # 2 retries
            try:
                session = await get_http_session()
                async with session.head(
                    url, 
                    allow_redirects=True, 
                    timeout=10, 
                    proxy=Config.PROXY
                ) as resp:
                    url = str(resp.url)
                    break
            except Exception:
                await asyncio.sleep(1)

    # 2. Extract Twitter direct media URL if it's a tweet link
    if any(domain in url.lower() for domain in ["twitter.com", "x.com", "t.co"]):
        # Resolve t.co first if necessary
        if "t.co" in url.lower():
            for _ in range(2):
                try:
                    session = await get_http_session()
                    async with session.head(
                        url, 
                        allow_redirects=True, 
                        timeout=10, 
                        proxy=Config.PROXY
                    ) as resp:
                        url = str(resp.url)
                        break
                except Exception:
                    await asyncio.sleep(1)
                
        # Now Check for twitter.com / x.com and try vxtwitter API
        match = re.search(r'(?:twitter\.com|x\.com)/(?:[^/]+/status/|status/|status/|/)([0-9]+)', url, re.IGNORECASE)
        if match:
            tweet_id = match.group(1)
            api_url = f"https://api.vxtwitter.com/x/status/{tweet_id}"
            for _ in range(2):
                try:
                    session = await get_http_session()
                    async with session.get(api_url, timeout=10, proxy=Config.PROXY) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            media_urls = data.get("mediaURLs", [])
                            if media_urls:
                                # We return the direct raw video/image URL instead of the twitter page!
                                return media_urls[0]
                    break
                except Exception:
                    await asyncio.sleep(1)

    return url


def smart_output_name(filename: str) -> str:
    """
    Remap known streaming extensions to the proper container extension.
    e.g. 'stream.m3u8' → 'stream.mp4'
    """
    stem, ext = os.path.splitext(filename)
    return stem + STREAMING_EXTENSIONS.get(ext.lower(), ext)

# ── yt-dlp integration ───────────────────────────────────────────────────────

try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

_YTDLP_EXTRACTORS = None

def _get_ytdlp_extractors():
    """Lazy-load and cache the yt-dlp extractors (excluding GenericIE fallback)."""
    global _YTDLP_EXTRACTORS
    if _YTDLP_EXTRACTORS is None:
        try:
            from yt_dlp.extractor import gen_extractors
            _YTDLP_EXTRACTORS = [e for e in gen_extractors() if e.IE_NAME != 'generic']
        except Exception as e:
            Config.LOGGER.error(f"Failed to load yt-dlp extractors: {e}")
            _YTDLP_EXTRACTORS = []
    return _YTDLP_EXTRACTORS

# Domains where yt-dlp should be used directly. Additions here bypass dynamic extract checks and Regex strictness.
YTDLP_DOMAINS = {
    "youtube.com", "youtu.be", "youtube-nocookie.com",
    "instagram.com",
    "twitter.com", "x.com", "t.co",
    "tiktok.com", "vm.tiktok.com",
    "facebook.com", "fb.watch", "fb.com",
    "reddit.com", "v.redd.it", "redd.it",
    "dailymotion.com", "dai.ly",
    "vimeo.com",
    "twitch.tv", "clips.twitch.tv",
    "soundcloud.com",
    "bilibili.com", "b23.tv",
    "pinterest.com", "pin.it",
    "streamable.com",
    "rumble.com",
    "odysee.com",
    "bitchute.com",
    "mixcloud.com",
}

# Domains where cobalt API can be used as an alternative/fallback
COBALT_DOMAINS = {
    "youtube.com", "youtu.be",
    "reddit.com", "v.redd.it", "redd.it",
}


def is_ytdlp_url(url: str) -> bool:
    """Return True if the URL belongs to a yt-dlp-supported platform dynamically."""
    if not YTDLP_AVAILABLE:
        return False
    # Explicitly exclude YouTube from yt-dlp path to force Cobalt usage
    if any(x in url.lower() for x in ["youtube.com", "youtu.be", "youtube-nocookie.com"]):
        return False
    try:
        host = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        # Step 1: Check Hardcoded fallback domains (handles shortened links like `t.co` and `fb.com/share`)
        if any(host == d or host.endswith("." + d) for d in YTDLP_DOMAINS):
            return True
        # Step 2: Dynamically query all yt-dlp extractors natively supported
        extractors = _get_ytdlp_extractors()
        return any(e.suitable(url) for e in extractors)
    except Exception:
        return False


def is_cobalt_url(url: str) -> bool:
    """Return True if the URL can be handled by cobalt API as a fallback."""
    if not Config.COBALT_API_URL:
        return False
    try:
        host = urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
        return any(host == d or host.endswith("." + d) for d in COBALT_DOMAINS)
    except Exception:
        return False


def cancel_button(user_id: int) -> InlineKeyboardMarkup:
    """Build a simple cancel button markup."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✖️ Cancel", callback_data=f"cancel:{user_id}")
    ]])


async def _safe_edit(msg, text: str, reply_markup=None):
    """Edit a Telegram message, silently ignoring all errors."""
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
    except Exception:
        pass


async def fetch_ytdlp_title(url: str) -> str | None:
    """
    Extract the video title from yt-dlp (no download).
    Returns a clean filename like 'My Video Title.mp4', or None on failure.
    """
    if not YTDLP_AVAILABLE:
        return None
    loop = asyncio.get_running_loop()

    def _fetch():
        try:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "force_ipv4": True, # Common fix for Connection Reset on VPS
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
            if Config.COOKIES_FILE and os.path.exists(Config.COOKIES_FILE):
                opts["cookiefile"] = Config.COOKIES_FILE
            if Config.PROXY:
                opts["proxy"] = Config.PROXY

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                title = info.get("title") or info.get("id") or "video"
                title = re.sub(r'[\\/*?"<>|:\n\r\t]', "_", title).strip()
                ext = info.get("ext") or "mp4"
                return f"{title[:80]}.{ext}"
        except Exception:
            return None

    return await loop.run_in_executor(None, _fetch)


async def fetch_http_filename(url: str, default_name: str = "downloaded_file") -> str:
    """
    Probe a direct URL with a HEAD request to extract the true filename from Content-Disposition
    or guess the extension from the Content-Type.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        session = await get_http_session()
        async with session.head(
            url, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=10),
            proxy=Config.PROXY
        ) as head:
            mime = head.headers.get("Content-Type", "").split(";")[0].strip()
            cd = head.headers.get("Content-Disposition", "")
            # Check server-provided exact filename
            cd_match = re.search(r'filename="?([^"]+)"?', cd)
            if cd_match:
                return smart_output_name(cd_match.group(1))

            # If no Content-Disposition, check if the parsed URL name lacks an extension
            parsed_name = urllib.parse.unquote(os.path.basename(urllib.parse.urlparse(url).path.rstrip("/")))
            base_name = parsed_name if parsed_name else default_name
            
            if not os.path.splitext(base_name)[1]:
                ext = mimetypes.guess_extension(mime)
                if ext:
                    if ext == '.jpe': ext = '.jpg'
                    base_name += ext
                    
            return smart_output_name(base_name)
    except Exception:
        # Fallback to standard URL parsing
        parsed = urllib.parse.urlparse(url)
        name = os.path.basename(parsed.path.rstrip("/"))
        return smart_output_name(urllib.parse.unquote(name) if name else default_name)


async def get_best_filename(url: str, default_name: str = "downloaded_file") -> str:
    """
    Universally determine the best filename for any given URL.
    Routes to yt-dlp native extraction first, falling back to HTTP header sniffing for direct routes.
    """
    if is_ytdlp_url(url):
        ytdlp_title = await fetch_ytdlp_title(url)
        if ytdlp_title:
            return ytdlp_title
        # Even if it's a yt-dlp URL, if the title extraction fails (like on Pinterest), fall back
    
    # If it's a cobalt URL, Cobalt handles social media links so HTTP probes usually just return HTML.
    # So we don't bother probing Cobalt URLs, just return the parsed stem + default .mp4
    if is_cobalt_url(url):
        parsed = urllib.parse.urlparse(url)
        name = os.path.basename(parsed.path.rstrip("/"))
        base_name = urllib.parse.unquote(name) if name else default_name
        if not os.path.splitext(base_name)[1]:
            base_name += ".mp4"
        return smart_output_name(base_name)

    return await fetch_http_filename(url, default_name)


async def fetch_ytdlp_formats(url: str) -> dict:
    """
    Fetch available video formats from yt-dlp.
    Returns: {"formats": list[dict], "title": str}
    """
    if not YTDLP_AVAILABLE:
        return {"formats": [], "title": ""}
    loop = asyncio.get_running_loop()

    def _fetch():
        try:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "force_ipv4": True,
                "nocheckcertificate": True, # Ignore SSL artifacts
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }


            if Config.COOKIES_FILE and os.path.exists(Config.COOKIES_FILE):
                opts["cookiefile"] = Config.COOKIES_FILE
            if Config.PROXY:
                opts["proxy"] = Config.PROXY
            
            # NSFW / PornHub tweaks
            if "pornhub.com" in url:
                opts["referer"] = "https://www.pornhub.com/"
                opts["geo_bypass"] = True
                opts["socket_timeout"] = 20
                opts["extractor_args"] = {'pornhub': {'prefer_formats': 'mp4'}}

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                formats = info.get("formats", [])
                title = info.get("title", "video")
                
                # Find the best audio size to add to video-only stream sizes
                best_audio_size = 0
                for f in formats:
                    if f.get("vcodec") == "none" and f.get("acodec") != "none":
                        size = f.get("filesize") or f.get("filesize_approx") or 0
                        if size > best_audio_size:
                            best_audio_size = size

                # Filter for useful formats
                available = {}
                for f in formats:
                    height = f.get("height")
                    
                    # Facebook Missing Qualities Fix: HD/SD streams have height=None
                    if height is None:
                        fid = str(f.get("format_id", "")).lower()
                        if fid == "hd":
                            height = 720
                        elif fid == "sd":
                            height = 360
                    
                    if height and f.get("vcodec") != "none":
                        res = f"{height}p"
                        # yt-dlp returns formats sorted from worst to best.
                        # Overwriting the key guarantees we keep the best format for this resolution
                        available[res] = f
                
                results = []
                sorted_res = sorted(
                    available.keys(), 
                    key=lambda x: int(re.search(r'(\d+)p', x).group(1)) if re.search(r'(\d+)p', x) else 0, 
                    reverse=True
                )
                
                for res in sorted_res:
                    f = available[res]
                    size = f.get("filesize") or f.get("filesize_approx")
                    
                    # Add audio size only if we have a base video size and it lacks audio
                    if f.get("acodec") == "none" and size is not None and best_audio_size > 0:
                        size += best_audio_size
                        
                    results.append({
                        "format_id": f["format_id"],
                        "resolution": res,
                        "ext": f.get("ext", "mp4"),
                        "filesize": size  # Keep as None if unknown, UI handles `humanbytes(None) -> Unknown`
                    })
                
                # If we only found 1 distinct resolution, we return empty list so the bot skips selection
                if len(results) < 2:
                    return {"formats": [], "title": title}
                    
                return {"formats": results, "title": title}
        except Exception as e:
            Config.LOGGER.error(f"Error fetching formats for {url}: {e}")
            return {"formats": [], "title": ""}

    return await loop.run_in_executor(None, _fetch)

async def check_ffmpeg() -> bool:
    """Check if ffmpeg is available."""
    try:
        proc = await asyncio.create_subprocess_exec(
            _get_ffmpeg_bin(), "-version",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()
        return proc.returncode == 0
    except Exception:
        return False


async def download_ytdlp(
    url: str,
    filename: str,
    progress_msg,
    start_time_ref: list,
    user_id: int,
    format_id: str = None,
    cancel_ref: list = None,
) -> tuple[str, str]:
    """
    Download content using yt-dlp with live progress.
    Uses the user-supplied filename as the output stem.
    If format_id is provided, it attempts to download that specific format.
    Pass cancel_ref=[False] to safely abort via asyncio.
    Returns (file_path, mime_type).
    """
    start_time_ref[0] = time.time()
    loop = asyncio.get_running_loop()
    last_edit = [start_time_ref[0]]

    out_dir = Config.DOWNLOAD_LOCATION
    os.makedirs(out_dir, exist_ok=True)
    
    Config.LOGGER.info(f"download_ytdlp started for {user_id}. SyncID={id(WEBAPP_PROGRESS)}")

    # State update for WebApp
    WEBAPP_PROGRESS[user_id] = {
        "action": "Analyzing Media URL...",
        "percentage": 5,
        "current": "0 B",
        "total": "Fetching size...",
        "speed": "---"
    }

    # Build a safe output stem from the user-chosen filename
    # Shorten to 80 chars to avoid OS length limits (e.g. for long Facebook titles)
    safe_stem = re.sub(r'[\\/*?"<>|:]', "_", os.path.splitext(filename)[0])[:80]
    if not safe_stem:
        safe_stem = "video_file"
    outtmpl = os.path.join(out_dir, f"{safe_stem}.%(ext)s")

    def _progress_hook(d: dict):
        if cancel_ref and cancel_ref[0]:
            raise asyncio.CancelledError("Download cancelled by user.")

        done = d.get("downloaded_bytes", 0)
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or d.get("filesize") or d.get("filesize_approx", 0)
        speed = d.get("speed") or 0
        eta = d.get("eta") or 0
        percent = (done / total * 100) if total else 0

        # 1. Update global WebApp tracker FREQUENTLY
        WEBAPP_PROGRESS[user_id] = {
            "action": "Downloading Media...",
            "current": humanbytes(done),
            "total": humanbytes(total) if total else "Unknown",
            "speed": f"{humanbytes(speed)}/s" if speed else "",
            "percentage": max(1, round(percent, 1))
        }

        # 2. Update Telegram message INFREQUENTLY
        now = time.time()
        if d["status"] == "downloading" and now - last_edit[0] >= PROGRESS_UPDATE_DELAY:
            last_edit[0] = now
            bar = progress_bar(done, total) if total else "░" * 12
            pct = f"{done / total * 100:.1f}%" if total else "…"
            text = (
                f"📥 **Downloading Media…**\n\n"
                f"📁 **Name:** `{os.path.basename(outtmpl)}`\n"
                f"[{bar}] {pct}\n"
                f"**Done:** {humanbytes(done)}"
                + (f" / {humanbytes(total)}" if total else "")
                + (f"\n**Speed:** {humanbytes(speed)}/s" if speed else "")
                + (f"\n**ETA:** {time_formatter(eta)}" if eta else "")
            )
            asyncio.run_coroutine_threadsafe(
                _safe_edit(progress_msg, text, reply_markup=cancel_button(user_id)),
                loop
            )

    # ── Build format string based on ffmpeg availability ─────────────────────
    ffmpeg_available = await check_ffmpeg()

    if format_id == "best":
        # User requested max quality but wants to avoid slow FFmpeg merging times.
        # "best" explicitly asks yt-dlp for the best pre-merged video/audio file.
        fmt = "best[ext=mp4]/best"
    elif format_id:
        # If user picked a specific resolution, we try to get that video + best audio
        # or just that specific format if it's already merged.
        if ffmpeg_available:
            fmt = f"{format_id}+bestaudio/{format_id}/best"
        else:
            fmt = f"{format_id}/best"
    elif ffmpeg_available:
        # ffmpeg found → prefer best quality separate streams, merge to mp4
        fmt = (
            "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]"
            "/bestvideo[height<=1080]+bestaudio"
            "/best[height<=1080]/best"
        )
    else:
        # NO ffmpeg → only pick formats that are already a single file (no merge needed)
        fmt = (
            "best[height<=1080][ext=mp4]"
            "/best[height<=1080]"
            "/best"
        )

    ydl_opts = {
        "format": fmt,
        "outtmpl": outtmpl,
        "progress_hooks": [_progress_hook],
        "quiet": True,
        "no_warnings": True,
        "force_ipv4": True,
        "nocheckcertificate": True,
        "merge_output_format": "mp4",
        "overwrites": True,
        "noplaylist": True,
        "max_filesize": Config.MAX_FILE_SIZE,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "concurrent_fragment_downloads": 30, # Max out DASH/HLS fragment speeds
        "hls_prefer_native": True,           # Native HLS allows better progress reporting
        "buffersize": 1048576,               # 1MB Buffer for speed
        "retries": 10,
        "fragment_retries": 10,
    }


    # Set ffmpeg_location to the DIRECTORY, not the binary path
    ffmpeg_dir = _get_ffmpeg_dir()
    if ffmpeg_dir:
        ydl_opts["ffmpeg_location"] = ffmpeg_dir

    if Config.COOKIES_FILE and os.path.exists(Config.COOKIES_FILE):
        ydl_opts["cookiefile"] = Config.COOKIES_FILE

    if Config.PROXY:
        ydl_opts["proxy"] = Config.PROXY

    # Platform specific tweaks
    if "reddit.com" in url or "v.redd.it" in url:
        ydl_opts["referer"] = "https://www.reddit.com/"
    elif "pornhub.com" in url:
        ydl_opts["referer"] = "https://www.pornhub.com/"
        ydl_opts["geo_bypass"] = True
        ydl_opts["socket_timeout"] = 30
        ydl_opts["extractor_args"] = {'pornhub': {'prefer_formats': 'mp4'}}

    async def _run_async() -> str:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # 1. Extract metadata only
                info = await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=False))
                
                # Use safe_stem to build path (we prefer the user's filename)
                ext = info.get("ext", "mp4")
                target_path = os.path.join(out_dir, f"{safe_stem}.{ext}")
                
                # 2. Identify if handover to aria2c is possible (single direct file)
                # If requested_formats is missing or only has 1 format, it's usually a single file.
                req_formats = info.get("requested_formats")
                is_single = not req_formats or len(req_formats) == 1
                
                # Check protocol — avoid aria2 for complex streams (DASH/HLS) that yt-dlp handles better natively
                protocol = info.get("protocol", "")
                is_direct = "http" in protocol and "m3u8" not in protocol and "dash" not in protocol

                if is_single and is_direct:
                    Config.LOGGER.info(f"Handing off direct URL to aria2c for max speed: {url}")
                    # Forward headers (User-Agent, Cookie, Referer)
                    headers = info.get("http_headers", {})
                    # Ensure cookies from the cookiefile are included if provided by yt-dlp
                    return await _download_aria2c(
                        info["url"], target_path, progress_msg, start_time_ref, user_id, 
                        cancel_ref=cancel_ref, 
                        headers=headers
                    )
                
                # 3. Fallback: Use yt-dlp native downloader (with progress hooks)
                # This handles merging (video+audio) and fragmented streams (HLS/DASH)
                Config.LOGGER.info(f"Using native yt-dlp downloader for complex stream/merge: {url}")
                await loop.run_in_executor(None, lambda: ydl.process_info(info))
                
                # Merged mp4 is the most likely output
                mp4_path = os.path.join(out_dir, f"{safe_stem}.mp4")
                if os.path.exists(mp4_path):
                    return mp4_path
                # Fallback: find any file starting with the safe stem
                candidates = sorted(
                    [f for f in os.listdir(out_dir) if f.startswith(safe_stem)],
                    key=lambda f: os.path.getsize(os.path.join(out_dir, f)),
                    reverse=True,
                )
                if candidates:
                    return os.path.join(out_dir, candidates[0])
                raise FileNotFoundError("Error: output file not found after download")
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            Config.LOGGER.error(f"yt-dlp/aria2c critical error for {url}: {e}")
            raise

    file_path = await _run_async()
    mime = mimetypes.guess_type(file_path)[0] or "video/mp4"
    return file_path, mime


# ── Cobalt API fallback ──────────────────────────────────────────────────────

async def download_cobalt(
    url: str,
    filename: str,
    progress_msg,
    start_time_ref: list,
    user_id: int,
    cancel_ref: list = None,
) -> tuple[str, str]:
    """
    Download content using the cobalt API (fallback for Instagram/Pinterest).
    No cookies required — cobalt handles authentication independently.
    Returns (file_path, mime_type).
    """
    start_time_ref[0] = time.time()
    out_dir = Config.DOWNLOAD_LOCATION
    os.makedirs(out_dir, exist_ok=True)
    safe_stem = re.sub(r'[\\/*?"<>|:]', "_", os.path.splitext(filename)[0])[:80]

    api_url = Config.COBALT_API_URL.rstrip("/")
    payload = {
        "url": url,
        "downloadMode": "auto",
        "videoQuality": "1080",
        "filenameStyle": "basic",
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "TelegramBot/1.0",
    }

    try:
        await _safe_edit(
            progress_msg,
            "📥 **Initializing Download…** ⏳\n_Please wait while we prepare your file..._",
            reply_markup=cancel_button(user_id)
        )
        
        # Initial state for WebApp
        WEBAPP_PROGRESS[user_id] = {
            "action": "Requesting Extraction Server...",
            "percentage": 5,
            "current": "0 B",
            "total": "Unknown",
            "speed": "---"
        }

        session = await get_http_session()
        # Step 1: Ask cobalt for the download URL
        async with session.post(
            f"{api_url}/",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
            proxy=Config.PROXY
        ) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise ValueError(f"Download server returned {resp.status}: {error_text[:200]}")
            data = await resp.json()

            status = data.get("status")

            if status == "error":
                error_code = data.get("error", {}).get("code", "unknown")
                raise ValueError(f"Extraction error: {error_code}")

            # Get the download URL from the response
            if status in ("tunnel", "redirect"):
                download_url_str = data.get("url")
                cobalt_filename = data.get("filename", f"{safe_stem}.mp4")
            elif status == "picker":
                # Multiple items — take the first video/photo
                picker = data.get("picker", [])
                if not picker:
                    raise ValueError("No media found to extract")
                download_url_str = picker[0].get("url")
                cobalt_filename = f"{safe_stem}.mp4"
            else:
                raise ValueError(f"Download server returned unexpected status: {status}")

            if not download_url_str:
                raise ValueError("Could not extract media URL")

            # Determine output file extension from cobalt filename
            _, ext = os.path.splitext(cobalt_filename)
            if not ext:
                ext = ".mp4"
            out_path = os.path.join(out_dir, f"{safe_stem}{ext}")

            try:
                # Step 2: Download extremely fast via aria2c using the Cobalt proxy URL
                await _safe_edit(progress_msg, "📥 **Extracting Media…** ⚙️", reply_markup=cancel_button(user_id))
                
                # Transition state for WebApp
                WEBAPP_PROGRESS[user_id] = {
                    "action": "Starting Download...",
                    "percentage": 10,
                    "current": "0 B",
                    "total": "Fetching...",
                    "speed": "Waiting"
                }

                await _download_aria2c(download_url_str, out_path, progress_msg, start_time_ref, user_id, cancel_ref=cancel_ref)

            except Exception:
                if os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except Exception:
                        pass
                raise


            mime = mimetypes.guess_type(out_path)[0] or "video/mp4"
            return out_path, mime

    except Exception as e:
        raise ValueError(f"Download failed: {e}")


def humanbytes(size: int) -> str:
    if size is None or size < 0:
        return "Unknown"
    if not size:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            if unit in ["B", "KB"]:
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def time_formatter(seconds: float) -> str:
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    elif minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def progress_bar(current: int, total: int, length: int = 12) -> str:
    filled = int(length * current / total) if total else 0
    bar = "█" * filled + "░" * (length - filled)
    percent = current / total * 100 if total else 0
    return f"[{bar}] {percent:.1f}%"


# ── FFprobe / FFmpeg helpers ──────────────────────────────────────────────────

async def get_video_metadata(file_path: str) -> dict:
    """
    Use ffprobe (async subprocess) to extract duration, width, height from a video.
    Returns a dict with keys: duration (int seconds), width (int), height (int).
    Falls back to zeros if ffprobe is unavailable or fails.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            _get_ffprobe_bin(),
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        data = json.loads(stdout)
        video_stream = next(
            (s for s in data.get("streams", []) if s.get("codec_type") == "video"),
            None,
        )
        duration = int(float(data.get("format", {}).get("duration", 0)))
        width = int(video_stream.get("width", 0)) if video_stream else 0
        height = int(video_stream.get("height", 0)) if video_stream else 0
        return {"duration": duration, "width": width, "height": height}
    except Exception:
        return {"duration": 0, "width": 0, "height": 0}


async def generate_video_thumbnail(file_path: str, chat_id: int, duration: int = 0) -> str | None:
    """
    Extract a single frame from the video at 10% of its duration (or 1 s if unknown),
    scaled to max width 320 px, saved as JPEG.  Returns the path or None on failure.
    """
    thumb_path = os.path.join(Config.DOWNLOAD_LOCATION, f"thumb_auto_{chat_id}.jpg")
    # Pick a timestamp: 0 seconds (first frame) to avoid any seeking overhead and instantly grab the screen
    seek = 0
    try:
        proc = await asyncio.create_subprocess_exec(
            _get_ffmpeg_bin(),
            "-y",
            "-threads", "1",
            "-ss", str(seek),
            "-i", file_path,
            "-vframes", "1",
            "-vf", "scale=320:-1",
            "-q:v", "2",          # JPEG quality (2 = very high, 31 = worst)
            thumb_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await asyncio.wait_for(proc.communicate(), timeout=60)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception:
        pass
    return None


# ── Download helpers ──────────────────────────────────────────────────────────

async def _download_hls(url: str, out_path: str, progress_msg, start_time_ref: list, user_id: int, cancel_ref: list = None) -> str:
    """
    Use ffmpeg to download an HLS/DASH/TS stream and remux it to mp4.
    Shows elapsed-time progress (no size info available for streams).
    """
    start_time_ref[0] = time.time()
    last_edit = start_time_ref[0]

    proc = await asyncio.create_subprocess_exec(
        _get_ffmpeg_bin(), "-y",
        "-i", url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",   # fix AAC bitstream for mp4 container
        out_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    # Read stderr in background so pipe doesn't fill up and block ffmpeg
    stderr_chunks = []

    async def _read_stderr():
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            stderr_chunks.append(line)

    stderr_task = asyncio.create_task(_read_stderr())

    # Poll until ffmpeg finishes, editing progress every PROGRESS_UPDATE_DELAY s
    try:
        while True:
            try:
                await asyncio.wait_for(proc.wait(), timeout=1.0)
                break  # process finished
            except asyncio.TimeoutError:
                pass  # still running, update progress
            
            if cancel_ref and cancel_ref[0]:
                try:
                    proc.kill()
                except Exception:
                    pass
                raise asyncio.CancelledError("Upload cancelled.")

            now = time.time()
            if now - last_edit >= PROGRESS_UPDATE_DELAY:
                elapsed = now - start_time_ref[0]
                # Since HLS streaming size is unknown, we fake a pulsing progress bar in the UI
                pulsing_pct = min(((elapsed % 10) / 10) * 100, 99.9)
                WEBAPP_PROGRESS[user_id] = {
                    "action": "Weaving Stream together... 🧵",
                    "current": time_formatter(elapsed),
                    "total": "Unknown",
                    "speed": "Streaming",
                    "percentage": round(pulsing_pct, 1)
                }
                try:
                    await progress_msg.edit_text(
                        f"📥 **Weaving the stream together…** 🧵\n"
                        f"⏱ Elapsed: {time_formatter(elapsed)}",
                        reply_markup=cancel_button(user_id)
                    )
                except Exception:
                    pass
                last_edit = now

        await stderr_task  # ensure stderr is fully read

        if proc.returncode != 0:
            err_log = b"".join(stderr_chunks).decode(errors="replace")
            raise RuntimeError(f"ffmpeg stream download failed:\n{err_log[-600:] if err_log else 'Unknown error'}")

        return out_path
    except Exception:
        # Cleanup incomplete ffmpeg output if cancelled or failed
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        raise
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass


async def download_url(url: str, filename: str, progress_msg, start_time_ref: list, user_id: int, format_id: str = None, cancel_ref: list = None):
    """
    Stream-download a URL to disk, editing progress_msg periodically.
    Returns (path, mime_type) on success or raises.
    """
    download_dir = Config.DOWNLOAD_LOCATION
    os.makedirs(download_dir, exist_ok=True)

    # Remap streaming extensions to proper container (e.g. .m3u8 → .mp4)
    filename = smart_output_name(filename)
    # Shorten to 80 chars to avoid OS length limits
    safe_name = re.sub(r'[\\/*?:"<>|]', "_", filename)[:80]
    file_path = os.path.join(download_dir, safe_name)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }

    # ── Route yt-dlp-supported platforms ─────────────────────────────────────
    if is_ytdlp_url(url):
        try:
            status_text = "📥 **Doing some black magic…** 🪄\n_(connecting to the dark side…)_"
            await progress_msg.edit_text(status_text, reply_markup=cancel_button(user_id))
        except Exception:
            pass
        try:
            return await download_ytdlp(url, filename, progress_msg, start_time_ref, user_id, format_id=format_id, cancel_ref=cancel_ref)
        except Exception as ytdlp_err:
            if isinstance(ytdlp_err, asyncio.CancelledError):
                raise
            # If yt-dlp fails and cobalt supports this URL, try cobalt as fallback
            if is_cobalt_url(url):
                Config.LOGGER.info(
                    "Initial extraction failed, trying secondary servers..."
                )
                try:
                    return await download_cobalt(url, filename, progress_msg, start_time_ref, user_id, cancel_ref=cancel_ref)
                except Exception as cobalt_err:
                    # Both failed — raise the original yt-dlp error with cobalt context
                    raise ValueError(
                        f"Error 1: {ytdlp_err}\n\nError 2: {cobalt_err}"
                    ) from ytdlp_err
            else:
                raise  # re-raise yt-dlp error for non-cobalt URLs

    # Secondary extraction route: Force Cobalt for skipped yt-dlp domains (e.g. YouTube)
    if is_cobalt_url(url):
        try:
            return await download_cobalt(url, filename, progress_msg, start_time_ref, user_id, cancel_ref=cancel_ref)
        except Exception:
            pass # fall through to aria2c/http probe if cobalt fails

    # Transition state for WebApp
    WEBAPP_PROGRESS[user_id] = {
        "action": "Identifying Resource...",
        "percentage": 5,
        "current": "0 B",
        "total": "Checking...",
        "speed": "---"
    }

    # ── Probe the URL to detect content type ─────────────────────────────────
    session = await get_http_session()
    async with session.head(
        url, allow_redirects=True,
        timeout=aiohttp.ClientTimeout(total=30),
        proxy=Config.PROXY
    ) as head:
        mime = head.headers.get("Content-Type", "").split(";")[0].strip()
        total_str = head.headers.get("Content-Length", "0")
        total = int(total_str) if total_str.isdigit() else 0
        
        # Extract true filename if available from server
        cd = head.headers.get("Content-Disposition", "")
        cd_match = re.search(r'filename="?([^"]+)"?', cd)
        if cd_match:
            filename = cd_match.group(1)
        else:
            # If no Content-Disposition, and filename lacks an extension, guess via mime
            if not os.path.splitext(filename)[1]:
                ext = mimetypes.guess_extension(mime)
                if ext:
                    # Some systems return '.jpe' for jpeg
                    if ext == '.jpe': ext = '.jpg'
                    filename += ext

    # Re-evaluate safe filename based on true network name
    filename = smart_output_name(filename)
    safe_name = re.sub(r'[\\/*?:"<>|]', "_", filename)[:80]
    file_path = os.path.join(download_dir, safe_name)

    # Transition state for WebApp
    WEBAPP_PROGRESS[user_id] = {
        "action": "Starting Stream Download...",
        "percentage": 10,
        "current": "0 B",
        "total": humanbytes(total) if total else "Unknown",
        "speed": "---"
    }

    # ── Route HLS / DASH / TS streams through ffmpeg ──────────────────────────
    if needs_ffmpeg_download(url, mime):
        # Force mp4 output path
        mp4_path = os.path.splitext(file_path)[0] + ".mp4"
        try:
            await progress_msg.edit_text(
                "📥 **Downloading stream…**\n"
                "_(live stream detected — stitching it together…)_ 🧵",
                reply_markup=cancel_button(user_id)
            )
        except Exception:
            pass
        await _download_hls(url, mp4_path, progress_msg, start_time_ref, user_id, cancel_ref=cancel_ref)
        return mp4_path, "video/mp4"

    # ── Aria2c High Speed Download ──────────────────────────────────────────────
    if total > Config.MAX_FILE_SIZE:
        raise ValueError(
            f"File too large: {humanbytes(total)} (max {humanbytes(Config.MAX_FILE_SIZE)})"
        )

    await _download_aria2c(url, file_path, progress_msg, start_time_ref, user_id, cancel_ref=cancel_ref)

    mime_from_ext = mimetypes.guess_type(file_path)[0]
    final_mime = mime_from_ext or mime
    return file_path, final_mime



# ── Aria2c Custom Downloader ──────────────────────────────────────────────────

# Global aria2 client bound to the daemon we started in bot.py
aria2 = aria2p.API(
    aria2p.Client(
        host="http://localhost",
        port=6800,
        secret=""
    )
)

async def _download_aria2c(url: str, out_path: str, progress_msg, start_time_ref: list, user_id: int, cancel_ref: list = None, headers: dict = None) -> str:
    """
    Download extremely fast using aria2c native RPC daemon.
    Uses 16 concurrent HTTP streams and no file allocation for Koyeb disk stability.
    """
    start_time_ref[0] = time.time()
    last_edit = start_time_ref[0]
    
    # Ensure background aria2c daemon writes exactly where we expect
    out_path = os.path.abspath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    options = {
        "max-connection-per-server": "16",
        "split": "16",
        "min-split-size": "1M",
        "file-allocation": "none",
        "dir": os.path.dirname(out_path),
        "out": os.path.basename(out_path),
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "header": ["Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"]
    }
    
    # Merge custom headers (e.g. Cookies, Referer from yt-dlp)
    if headers:
        if "header" not in options:
            options["header"] = []
        for k, v in headers.items():
            # aria2 expects "Name: Value" strings in the header list
            options["header"].append(f"{k}: {v}")
        if "User-Agent" in headers:
            options["user-agent"] = headers["User-Agent"]

    # Add the download to the daemon
    download = await asyncio.to_thread(aria2.add_uris, [url], options)

    try:
        while True:
            await asyncio.to_thread(download.update)

            if download.is_complete:
                break

            if cancel_ref and cancel_ref[0]:
                await asyncio.to_thread(download.remove, force=True, files=True)
                raise asyncio.CancelledError("Download cancelled.")
            
            if download.has_failed:
                error_msg = download.error_message
                await asyncio.to_thread(download.remove, force=True, files=True)
                raise RuntimeError(f"aria2c download failed: {error_msg}")

            pct_int = int(download.progress)
            _bar = progress_bar(pct_int, 100)
            
            speed_str = download.download_speed_string()
            current_str = download.completed_length_string()
            total_str = download.total_length_string()

            # 1. Update global WebApp tracker FREQUENTLY (every 200ms)
            WEBAPP_PROGRESS[user_id] = {
                "action": "Downloading...",
                "current": current_str,
                "total": total_str,
                "speed": speed_str,
                "percentage": pct_int
            }

            # 2. Update Telegram message INFREQUENTLY (every 1s) to avoid flood
            now = time.time()
            if now - last_edit >= PROGRESS_UPDATE_DELAY:
                text = (
                    f"📥 **Downloading Media…** ⬇️\n\n"
                    f"📁 **Name:** `{os.path.basename(out_path)}`\n"
                    f"[{_bar}] {download.progress_string()}\n"
                    f"**Done:** {current_str} / {total_str}\n"
                    f"**Speed:** {speed_str}\n"
                    f"**ETA:** {download.eta_string()}"
                )
                try:
                    await progress_msg.edit_text(text, reply_markup=cancel_button(user_id))
                except Exception:
                    pass
                last_edit = now

            await asyncio.sleep(0.2) # High frequency polling for smooth UI

        return out_path
    except Exception:
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        raise



# ── Upload helper ─────────────────────────────────────────────────────────────

async def upload_file(
    client: Client,
    chat_id: int,
    file_path: str,
    mime: str,
    caption: str,
    thumb_file_id: str | None,
    progress_msg,
    start_time_ref: list,
    user_id: int,              # Explicit user_id for WEBAPP_PROGRESS tracking
    force_document: bool = False,
    cancel_ref: list = None,
):
    """
    Upload a local file to Telegram with:
    - Live progress bar
    - Correct duration / width / height for videos (extracted via ffprobe)
    - Auto-generated thumbnail from the video frame if no custom thumb is set
    - Custom thumbnail (downloaded from Telegram by file_id) if set by user
    """

    last_edit = [time.time()]
    start_time_ref[0] = time.time()

    async def _progress(current: int, total: int):
        if cancel_ref and cancel_ref[0]:
            raise asyncio.CancelledError("Upload cancelled.")
            
        now = time.time()
        done = current
        percent = (done / total * 100) if total else 0
        elapsed = now - start_time_ref[0]
        speed = done / elapsed if elapsed else 0
        eta = (total - done) / speed if speed else 0
        
        # 1. Update global WebApp tracker FREQUENTLY
        WEBAPP_PROGRESS[user_id] = {
            "action": "Uploading to Telegram...",
            "current": humanbytes(done),
            "total": humanbytes(total) if total else "Unknown",
            "speed": f"{humanbytes(speed)}/s" if speed else "",
            "percentage": round(percent, 1)
        }

        # 2. Update Telegram message INFREQUENTLY
        if now - last_edit[0] < PROGRESS_UPDATE_DELAY:
            return
            
        bar = progress_bar(done, total)
        text = (
            "📤 **Uploading…**\n\n"
            f"📁 **Name:** `{os.path.basename(file_path)}`\n"
            f"{bar}\n"
            f"**Done:** {humanbytes(done)} / {humanbytes(total)}\n"
            f"**Speed:** {humanbytes(speed)}/s\n"
            f"**ETA:** {time_formatter(eta)}"
        )
        try:
            await progress_msg.edit_text(text, reply_markup=cancel_button(chat_id))
        except Exception:
            pass
        last_edit[0] = now

    os.makedirs(Config.DOWNLOAD_LOCATION, exist_ok=True)
    is_video = not force_document and bool(mime and mime.startswith("video/"))
    is_audio = not force_document and bool(mime and mime.startswith("audio/"))
    is_image = not force_document and bool(mime and mime.startswith("image/"))

    # ── 1. Get video metadata (duration, width, height) ───────────────────────
    meta = {"duration": 0, "width": 0, "height": 0}
    if is_video:
        try:
            await progress_msg.edit_text("🔍 Reading video metadata…", reply_markup=cancel_button(chat_id))
        except Exception:
            pass
        meta = await get_video_metadata(file_path)

    # Truncate caption to Telegram limit (1024)
    if caption and len(caption) > 1000:
        caption = caption[:997] + "..."

    # ── 2. Resolve thumbnail ───────────────────────────────────────────────────
    # Use a unique thumb name to avoid conflicts during concurrent uploads
    thumb_suffix = abs(hash(file_path)) % 10000
    thumb_local = None

    if thumb_file_id:
        try:
            thumb_local = await client.download_media(
                thumb_file_id,
                file_name=os.path.join(Config.DOWNLOAD_LOCATION, f"thumb_user_{chat_id}_{thumb_suffix}.jpg"),
            )
        except Exception:
            thumb_local = None

    if not thumb_local and is_video:
        try:
            await progress_msg.edit_text("🖼️ Generating fast thumbnail…", reply_markup=cancel_button(chat_id))
        except Exception:
            pass
        # Ensure duration is at least 1s for better thumbnail compatibility
        v_duration = max(1, meta["duration"])
        thumb_local = await generate_video_thumbnail(file_path, f"{chat_id}_{thumb_suffix}", v_duration)

    # ── 3. Build kwargs (chat_id and file passed as positional args) ───────────
    kwargs = dict(
        caption=caption,
        parse_mode=None,
        progress=_progress,
    )
    if thumb_local:
        kwargs["thumb"] = thumb_local

    # ── 4. Send to Telegram ───────────────────────────────────────────────────
    try:
        # Final safety checks for metadata types
        v_duration = int(meta.get("duration", 0))
        v_width = int(meta.get("width", 0))
        v_height = int(meta.get("height", 0))

        if force_document:
            await client.send_document(chat_id, file_path, **kwargs)
        elif is_video:
            await client.send_video(
                chat_id,
                file_path,
                duration=v_duration,
                width=v_width,
                height=v_height,
                supports_streaming=True,
                **kwargs,
            )
        elif is_audio:
            await client.send_audio(chat_id, file_path, **kwargs)
        elif is_image:
            img_kwargs = dict(caption=caption, progress=_progress)
            if thumb_local:
                img_kwargs["thumb"] = thumb_local
            await client.send_photo(chat_id, file_path, **img_kwargs)
        else:
            await client.send_document(chat_id, file_path, **kwargs)
    except Exception as e:
        Config.LOGGER.error(f"Critical Pyrogram send error for {file_path}: {e}")
        # Log more info to help debug serialization issues
        Config.LOGGER.error(f"Metadata: {meta}, Thumb: {thumb_local}, Caption Len: {len(caption) if caption else 0}")
        raise
    finally:
        # Clean up any temp thumbnail files
        if thumb_local and os.path.exists(thumb_local):
            try:
                os.remove(thumb_local)
            except Exception:
                pass
