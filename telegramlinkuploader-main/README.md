# 🤖 Telegram URL Uploader Bot

Upload files up to **2 GB** to Telegram from any URL — including YouTube, Instagram, TikTok and 700+ more platforms. Built with [Pyrogram](https://docs.pyrogram.org/) (MTProto) for large file support, deployable on **Koyeb**.

---

## ✨ Features

| Feature | Details |
|---|---|
| 📤 Direct URL Upload | Send any direct download URL — bot downloads & uploads |
| 📺 yt-dlp Integration | Download from YouTube, Instagram, TikTok, Twitter/X, Reddit, Facebook, Vimeo + 700 more |
| ✏️ File Renaming | Bot asks for a new filename before every upload |
| 🎬 Media / Document mode | Choose to send as streamable video or raw document |
| 🎞️ Auto Thumbnail | ffmpeg auto-generates thumbnail from video frame |
| ⏱️ Video Metadata | ffprobe extracts duration, width, height for proper Telegram video display |
| 🌊 HLS / DASH streams | `.m3u8`, `.mpd`, `.ts` streamed via ffmpeg → saved as `.mp4` |
| 💾 Up to 2 GB | Pyrogram MTProto — not the 50 MB Bot API limit |
| 🚀 Upload Boost | pyroblack `upload_boost=True` + 5 parallel MTProto connections |
| 📝 Custom Captions | Per-user saved captions |
| 🖼️ Permanent Thumbnails | Stored as Telegram `file_id` — survive restarts & redeployments |
| 📊 Live Progress | Download & upload progress bars with speed and ETA |
| 📢 Broadcast | Send messages to all users (admin) |
| 🚫 Ban / Unban | User management (admin) |
| ☁️ Koyeb Optimized | Startup validation, stdout logging, cookie env var, Docker ffmpeg |
| 🛡️ Proxy Support | Bypass IP-based rate limits on Instagram/Pinterest |
| 🍪 Cookie Auth | Use `COOKIES_DATA` env var or `cookies.txt` for authenticated downloads |
| 🔄 Cobalt API Fallback | Auto-retries Instagram/Pinterest/TikTok via [cobalt](https://github.com/imputnet/cobalt) when yt-dlp fails — no cookies needed |
| 🎯 Smart Format Selection | Best quality + ffmpeg merge when available; pre-merged fallback without it |

---

## 🌐 Supported Platforms (yt-dlp)

YouTube · Instagram · Twitter / X · TikTok · Facebook · Reddit · Vimeo · Dailymotion · Twitch · SoundCloud · Bilibili · Rumble · Odysee · Streamable · Mixcloud · Pinterest + [700 more](https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md)

---

## 🚀 Bot Commands

```
/start           – Check if bot is alive 🔔
/help            – Show all commands ❓
/about           – Bot info ℹ️
/upload <url>    – Upload file from URL 📤
/skip            – Keep original filename during rename

/caption <text>  – Set custom upload caption 📝
/showcaption     – View your caption
/clearcaption    – Clear caption

/setthumb        – Reply to a photo to set permanent thumbnail 🖼️
/showthumb       – Preview your thumbnail
/delthumb        – Delete thumbnail

--- Admin only ---
/broadcast <msg> – Broadcast to all users 📢
/total           – Total registered users 👥
/ban <id>        – Ban a user ⛔
/unban <id>      – Unban a user ✅
/status          – CPU / RAM / Disk stats + FFmpeg detection 🚀
```

---

## ⚙️ Environment Variables

Copy `.env.example` to `.env` and fill in:

### Required

| Variable | Description |
|---|---|
| `BOT_TOKEN` | From [@BotFather](https://t.me/BotFather) |
| `API_ID` | From [my.telegram.org](https://my.telegram.org) |
| `API_HASH` | From [my.telegram.org](https://my.telegram.org) |
| `OWNER_ID` | Your Telegram user ID |
| `DATABASE_URL` | MongoDB Atlas connection string |
| `LOG_CHANNEL` | Private channel ID for upload logs (negative number) |

### Optional

| Variable | Default | Description |
|---|---|---|
| `BOT_USERNAME` | `UrlUploaderBot` | Bot username (without @) |
| `UPDATES_CHANNEL` | _(none)_ | Updates channel username — button shown only if set |
| `ADMIN` | _(none)_ | Space-separated extra admin user IDs |
| `SESSION_STRING` | _(none)_ | Pyrogram session string for 4 GB uploads (premium account) |
| `CHUNK_SIZE` | `512` | Download chunk size in KB |

### Koyeb / Cloud

| Variable | Default | Description |
|---|---|---|
| `COOKIES_DATA` | _(none)_ | Paste full `cookies.txt` content here — bot auto-converts `\n` to real newlines on startup |
| `PROXY` | _(none)_ | Proxy URL: `http://user:pass@host:port` or `socks5://...` |
| `FFMPEG_PATH` | `/usr/bin/ffmpeg` | Path to ffmpeg binary — pre-set in Docker image, auto-detected otherwise |
| `COBALT_API_URL` | `https://permanent-coral-akila-519a0c52.koyeb.app` | Cobalt API endpoint for Instagram/Pinterest fallback |

---

## 🐳 Local Setup

```bash
git clone https://github.com/akilaramal69-beep/telegramlinkuploader.git
cd telegramlinkuploader

cp .env.example .env
# Fill in your values in .env

pip install -r requirements.txt
python bot.py
```

> **Requires:** `ffmpeg` and `ffprobe` installed on the system (included automatically in the Docker image).

---

## ☁️ Deploy to Koyeb

### Method 1 — Docker (recommended)

1. Fork this repo on GitHub
2. Go to [koyeb.com](https://www.koyeb.com) → **Create Service** → **Docker**
3. Use **GitHub** source and enable Docker build
4. Add all **Required** environment variables
5. Set **Port** to `8080` (health check endpoint: `/health`)
6. Deploy! ✅

### Method 2 — GitHub + Buildpack

1. Connect your GitHub repo to Koyeb
2. Build Command: `pip install -r requirements.txt`
3. Run Command: `python bot.py`
4. Port: `8080`
5. Add env vars → Deploy ✅

### Koyeb Tips

- **Bot won't start?** Check runtime logs — the bot validates `BOT_TOKEN`, `API_ID`, and `API_HASH` at startup and shows which ones are missing.
- **Instagram/Pinterest still failing?** The bot now **automatically falls back to cobalt API** when yt-dlp fails — no cookies needed. If the public cobalt API is blocked, self-host cobalt and set `COBALT_API_URL`.
- **Reddit/Facebook videos have no audio?** ffmpeg is pre-installed in the Docker image with `FFMPEG_PATH` pre-set. No extra config needed.
- **Bot logs** go to Koyeb's runtime console automatically (stdout only, no file logging).

### Download Flow

```
User sends URL
    │
    ├─ yt-dlp supported?  ───  ① Try yt-dlp (with cookies if available)
    │                             │
    │                         ❌ fails?
    │                             │
    │                         ② Auto-retry via cobalt API (no cookies)
    │
    ├─ HLS/DASH stream?   ───  Download via ffmpeg
    │
    └─ Direct URL         ───  Download via aiohttp
```

---

## 📁 Project Structure

```
telegramlinkuploader/
├── bot.py                  # Entrypoint: Pyrogram client + Flask health thread
├── app.py                  # Flask health server (port 8080)
├── requirements.txt
├── Dockerfile
├── .env.example
└── plugins/
    ├── config.py           # All config from environment variables
    ├── commands.py         # User commands, rename flow, mode selection
    ├── admin.py            # Admin commands (broadcast, ban, status)
    └── helper/
        ├── upload.py       # Download (aiohttp/yt-dlp/ffmpeg) + upload logic
        └── database.py     # MongoDB async helper (motor)
```

---

## 📝 Notes

- **Dual-engine downloads**: yt-dlp is tried first; if it fails for Instagram/Pinterest/TikTok/Twitter, the bot auto-retries via the cobalt API (no cookies required).
- **Startup validation**: The bot checks for required env vars (`BOT_TOKEN`, `API_ID`, `API_HASH`) and exits with a clear error if any are missing.
- **2 GB limit** via Pyrogram's MTProto API. The standard HTTP Bot API caps at 50 MB.
- **4 GB uploads** (Telegram Premium) require a `SESSION_STRING` of a premium account.
- **yt-dlp format selection** is adaptive: best quality streams + ffmpeg merge when available; pre-merged fallback without it.
- **HLS/DASH streams** (`.m3u8`, `.mpd`, `.ts`) are downloaded and remuxed to `.mp4` via ffmpeg.
- Files are downloaded to `./DOWNLOADS/` and deleted immediately after upload.
- **Filename safety**: Filenames are capped at 80 characters and sanitized for all filesystems.
- Thumbnails are stored as Telegram `file_id` strings in MongoDB — no local files needed.
