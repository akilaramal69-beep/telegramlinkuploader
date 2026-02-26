import asyncio
import os
import re
import time
import urllib.parse
import mimetypes
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from plugins.config import Config
from utils.shared import bot_client, WEBAPP_PROGRESS
from plugins.helper.database import add_user, get_user, update_user, is_banned
from plugins.helper.upload import (
    download_url, upload_file, humanbytes,
    smart_output_name, is_ytdlp_url, fetch_ytdlp_title,
    fetch_ytdlp_formats, get_best_filename, resolve_url
)

# ─────────────────────────────────────────────────────────────────────────────
# State dicts
#   PENDING_RENAMES: waiting for user to provide new filename
#   PENDING_MODE:    filename resolved, waiting for Media vs Document choice
#   PENDING_FORMATS: filename resolved, waiting for quality choice (yt-dlp only)
# ─────────────────────────────────────────────────────────────────────────────
PENDING_RENAMES: dict[int, dict] = {}   # {user_id: {"url": str, "orig": str}}
PENDING_MODE: dict[int, dict] = {}      # {user_id: {"url": str, "filename": str, "format_id": str}}
PENDING_FORMATS: dict[int, dict] = {}   # {user_id: {"url": str, "filename": str}}
ACTIVE_TASKS: dict[int, asyncio.Task] = {} # {user_id: Task}


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_filename(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    name = os.path.basename(parsed.path.rstrip("/"))
    return urllib.parse.unquote(name) if name else "downloaded_file"


HELP_TEXT = """
📋 **Bot Commands**

➤ /start – Check if the bot is alive 🔔
➤ /help – Show this help message ❓
➤ /about – Info about the bot ℹ️
➤ /upload `<url>` – Upload a file from a direct URL 📤
➤ /skip – Keep original filename (use after /upload)

**Caption:**
➤ /caption `<text>` – Set a custom caption for uploads 📝
➤ /showcaption – View your current caption
➤ /clearcaption – Remove your custom caption

**Thumbnail:**
➤ /setthumb – Reply to a photo to set thumbnail 🖼️
➤ /showthumb – View your current thumbnail
➤ /delthumb – Delete your saved thumbnail

**Admin only:**
➤ /broadcast `<msg>` – Broadcast to all users 📢
➤ /total – Total registered users 👥
➤ /ban `<id>` – Ban a user ⛔
➤ /unban `<id>` – Unban a user ✅
➤ /status – Bot resource usage 🚀

**Supported platforms:**
YouTube · Instagram · Twitter/X · TikTok · Facebook · Reddit
Vimeo · Dailymotion · Twitch · SoundCloud · Bilibili + more
"""

ABOUT_TEXT = """
🤖 **URL Uploader Bot**

Upload files up to **2 GB** directly to Telegram from any direct URL.

**Features:**
• ✏️ Rename files before upload
• 🎬 Choose Media or Document upload mode
• 🖼️ Permanent thumbnails (saved to your account)
• 📝 Custom captions
• 📊 Live progress bars

**Tech:** Pyrogram MTProto · MongoDB · Docker · Koyeb
"""


# ─────────────────────────────────────────────────────────────────────────────
#  Build the Mode-selection keyboard
# ─────────────────────────────────────────────────────────────────────────────

def mode_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Media", callback_data=f"mode:{user_id}:media"),
            InlineKeyboardButton("📄 Document", callback_data=f"mode:{user_id}:doc"),
        ]
    ])


def quality_keyboard(user_id: int, formats: list) -> InlineKeyboardMarkup:
    """Build a keyboard for selecting video resolution."""
    buttons = []
    # Add resolutions in rows of 2
    for i in range(0, len(formats), 2):
        row = []
        for f in formats[i:i+2]:
            size_val = f.get('filesize', 0)
            size_str = humanbytes(size_val) if size_val > 0 else "Unknown Size"
            label = f"{f['resolution']} ({size_str})"
            row.append(InlineKeyboardButton(label, callback_data=f"qual:{user_id}:{f['format_id']}"))
        buttons.append(row)
    # Add a "Best Quality" button at the end
    best_fmt = formats[0]['format_id'] if formats else "best"
    buttons.append([InlineKeyboardButton("✨ Best Quality (Auto)", callback_data=f"qual:{user_id}:best_{best_fmt}")])
    return InlineKeyboardMarkup(buttons)


async def ask_mode(target_msg: Message, user_id: int, filename: str):
    """Edit or reply with the upload-mode selection prompt."""
    text = (
        f"📁 **File:** `{filename}`\n\n"
        "How should this file be uploaded?"
    )
    try:
        await target_msg.edit_text(text, reply_markup=mode_keyboard(user_id))
    except Exception:
        await target_msg.reply_text(text, reply_markup=mode_keyboard(user_id), quote=True)


# ─────────────────────────────────────────────────────────────────────────────
#  Core upload executor
# ─────────────────────────────────────────────────────────────────────────────

async def do_upload(
    client: Client,
    reply_to: Message,
    user_id: int,
    url: str,
    filename: str,
    force_document: bool = False,
    format_id: str = None,
):
    """Wrapper to run the actual upload logic as a cancellable task."""
    cancel_ref = [False]
    task = asyncio.create_task(_do_upload_logic(
        client, reply_to, user_id, url, filename, cancel_ref, force_document, format_id
    ))
    # Combine task and cancel_ref so we can trigger True upon cancel button press
    ACTIVE_TASKS[user_id] = (task, cancel_ref)
    try:
        await task
    except asyncio.CancelledError:
        # Cleanup is handled in finally block or within _do_upload_logic
        pass
    finally:
        ACTIVE_TASKS.pop(user_id, None)


async def _do_upload_logic(
    client: Client,
    reply_to: Message,         # message to reply status updates into
    user_id: int,              # real user id (NOT from reply_to.from_user)
    url: str,
    filename: str,
    cancel_ref: list,
    force_document: bool = False,
    format_id: str = None,
):
    # status_msg already exists as the 'reply_to' message from trigger_webapp_download
    status_msg = reply_to
    
    # Heartbeat Step 2: Preparing status message
    WEBAPP_PROGRESS[user_id] = {
        "action": "Preparing Bot Context...",
        "percentage": 5,
        "current": "0 B",
        "total": "Unknown",
        "speed": "---"
    }
    Config.LOGGER.info(f"_do_upload_logic starting for {user_id}. SyncID={id(WEBAPP_PROGRESS)}")
    
    # Heartbeat Step 3: Entering Engine
    WEBAPP_PROGRESS[user_id] = {
        "action": "Handing off to Engine...",
        "percentage": 4,
        "current": "0 B",
        "total": "Unknown",
        "speed": "---"
    }
    Config.LOGGER.info(f"_do_upload_logic heartbeat 2/3 for {user_id}")

    start_time = [time.time()]
    file_path = None
    try:
        Config.LOGGER.info(f"calling download_url for {user_id}")
        file_path, mime = await download_url(url, filename, status_msg, start_time, user_id, format_id=format_id, cancel_ref=cancel_ref)
        file_size = os.path.getsize(file_path)

        # ── User settings ──────────────────────────────────────────────────
        user_data = await get_user(user_id) or {}
        custom_caption = user_data.get("caption") or ""
        thumb_file_id = user_data.get("thumb") or None

        caption = custom_caption or os.path.basename(file_path)

        await status_msg.edit_text("📤 Uploading to Telegram…")
        await upload_file(
            client, reply_to.chat.id, file_path, mime,
            caption, thumb_file_id, status_msg, start_time,
            user_id=user_id,
            force_document=force_document,
            cancel_ref=cancel_ref,
        )
        await status_msg.edit_text("✅ Upload complete!")
        
        # Signal the WebApp that the file is ready in chat
        WEBAPP_PROGRESS[user_id] = {
            "action": "Complete",
            "percentage": 100,
            "url": "tg://resolve?domain=linktotelebot" 
        }

        # ── Log ────────────────────────────────────────────────────────────
        if Config.LOG_CHANNEL:
            elapsed = time.time() - start_time[0]
            try:
                await client.send_message(
                    Config.LOG_CHANNEL,
                    f"📤 **Upload log**\n"
                    f"👤 `{user_id}`\n"
                    f"🔗 `{url}`\n"
                    f"📁 `{os.path.basename(file_path)}`\n"
                    f"💾 {humanbytes(file_size)} · ⏱ {elapsed:.1f}s\n"
                    f"📦 Mode: {'Document' if force_document else 'Media'}\n"
                    f"🎯 Format: {format_id or 'Auto'}",
                )
            except Exception:
                pass

    except asyncio.CancelledError:
        try:
            await status_msg.edit_text("❌ **Process cancelled by user.**")
        except Exception:
            pass
    except ValueError as e:
        await status_msg.edit_text(f"❌ {e}")
        WEBAPP_PROGRESS[user_id] = {
            "action": f"Error: {e}",
            "percentage": 0
        }
    except Exception as e:
        Config.LOGGER.exception("Upload error")
        await status_msg.edit_text(f"❌ Error: `{e}`")
        WEBAPP_PROGRESS[user_id] = {
            "action": f"Error: {e}",
            "percentage": 0
        }
    finally:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
#  Shared rename resolver — called after filename is decided
# ─────────────────────────────────────────────────────────────────────────────

async def resolve_rename(
    client: Client,
    prompt_msg: Message,   # the bot's rename prompt message
    user_id: int,
    url: str,
    filename: str,
):
    """Proceed to quality selection (if yt-dlp) or mode selection."""
    if is_ytdlp_url(url):
        try:
            await prompt_msg.edit_text("🔍 **Analyzing available qualities…**")
        except Exception:
            pass
        
        # Only shows quality selector if there are AT LEAST 2 distinct resolutions to choose from
        res = await fetch_ytdlp_formats(url)
        formats = res.get("formats")
        if formats:
            PENDING_FORMATS[user_id] = {"url": url, "filename": filename}
            try:
                await prompt_msg.edit_text(
                    f"🎬 **Select Resolution:**\n`{filename}`",
                    reply_markup=quality_keyboard(user_id, formats)
                )
                return
            except Exception:
                pass

    # Fallback/Direct loop: move straight to mode selection
    PENDING_MODE[user_id] = {"url": url, "filename": filename, "format_id": None}
    await ask_mode(prompt_msg, user_id, filename)


# ─────────────────────────────────────────────────────────────────────────────
#  /start
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user = message.from_user
    await add_user(user.id, user.username)
    if await is_banned(user.id):
        return await message.reply_text("🚫 You are banned from using this bot.")

    from pyrogram.types import WebAppInfo
    app_url = "https://swift-vilhelmina-akila-10dce4a8.koyeb.app" # The hosted URL of our Flask server
    
    buttons = []
    # Add the primary Mini App Launch Button
    buttons.append([InlineKeyboardButton("📱 Open Web Interface 📱", web_app=WebAppInfo(url=app_url))])
    
    if Config.UPDATES_CHANNEL:
        buttons.append([InlineKeyboardButton("📢 Updates", url=f"https://t.me/{Config.UPDATES_CHANNEL}")])
    buttons.append([InlineKeyboardButton("❓ Help", callback_data="help"),
                    InlineKeyboardButton("ℹ️ About", callback_data="about")])
    kb = InlineKeyboardMarkup(buttons)
    
    welcome_text = (
        f"👋 Hello **{user.first_name}**!\n\n"
        "I can upload files up to **2 GB** to Telegram from any direct URL.\n\n"
    )
    
    if not Config.ALLOW_BOT_URL_UPLOAD and user.id != Config.OWNER_ID and user.id not in Config.ADMIN:
        welcome_text += (
            "⚠️ **Direct bot uploads are currently disabled for users.**\n"
            "Please use the **Web Interface** button below to submit your links!"
        )
    else:
        welcome_text += (
            "📱 **Click the big Web Interface button below to try the new Mini App!**\n"
            "(You can also send a URL or use `/upload <url>` normally without it)."
        )

    await message.reply_text(
        welcome_text,
        reply_markup=kb,
        quote=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /help  /about
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message):
    await message.reply_text(HELP_TEXT, quote=True)


@Client.on_message(filters.command("about") & filters.private)
async def about_handler(client: Client, message: Message):
    await message.reply_text(ABOUT_TEXT, quote=True)


# ─────────────────────────────────────────────────────────────────────────────
#  Inline keyboard callbacks  — MUST use specific filters to avoid conflicts
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^(help|about)$"))
async def cb_help_about(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    if data == "help":
        await callback_query.message.edit_text(HELP_TEXT)
    elif data == "about":
        await callback_query.message.edit_text(ABOUT_TEXT)
    await callback_query.answer()


@Client.on_callback_query(filters.regex(r"^qual:(\d+):(.+)$"))
async def cb_quality(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split(":")
    target_id = int(parts[1])
    format_id = parts[2]  # "best" or specific id

    if user_id != target_id:
        return await callback_query.answer("Not your upload!", show_alert=True)

    pending = PENDING_FORMATS.pop(user_id, None)
    if not pending:
        return await callback_query.answer("Already processed or expired.", show_alert=True)

    await callback_query.answer()
    
    if format_id.startswith("best_"):
        chosen_label = "Best (Auto)"
        format_id = "best"
    else:
        chosen_label = format_id

    try:
        await callback_query.message.edit_text(f"✅ Quality: **{chosen_label}**")
    except Exception:
        pass
    
    # Store choice and move to mode selection
    PENDING_MODE[user_id] = {
        "url": pending["url"],
        "filename": pending["filename"],
        "format_id": format_id
    }
    await ask_mode(callback_query.message, user_id, pending["filename"])


@Client.on_callback_query(filters.regex(r"^cancel:(\d+)$"))
async def cb_cancel(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    target_id = int(callback_query.data.split(":")[1])

    if user_id != target_id:
        return await callback_query.answer("Not your process!", show_alert=True)

    task_info = ACTIVE_TASKS.get(user_id)
    if not task_info:
        return await callback_query.answer("No active process to cancel.", show_alert=True)

    task, cancel_ref = task_info
    cancel_ref[0] = True  # Signal yt-dlp/ffmpeg to abort
    task.cancel()         # Signal asyncio to abort
    await callback_query.answer("Cancelling process…")


@Client.on_callback_query(filters.regex(r"^skip_rename:(\d+)$"))
async def skip_rename_cb(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    target_id = int(callback_query.data.split(":")[1])
    if user_id != target_id:
        return await callback_query.answer("Not your upload!", show_alert=True)

    pending = PENDING_RENAMES.pop(user_id, None)
    if not pending:
        return await callback_query.answer("Already processed or expired.", show_alert=True)

    await callback_query.answer()
    # Move to mode selection
    await resolve_rename(
        client,
        callback_query.message,   # the rename prompt message to edit in place
        user_id,
        pending["url"],
        pending["orig"],
    )


@Client.on_callback_query(filters.regex(r"^mode:(\d+):(media|doc)$"))
async def mode_cb(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split(":")
    target_id = int(parts[1])
    choice = parts[2]   # "media" or "doc"

    if user_id != target_id:
        return await callback_query.answer("Not your upload!", show_alert=True)

    pending = PENDING_MODE.pop(user_id, None)
    if not pending:
        return await callback_query.answer("Already processed or expired.", show_alert=True)

    await callback_query.answer()
    mode_label = "📄 Document" if choice == "doc" else "🎬 Media"
        
    try:
        await callback_query.message.edit_text(
            f"✅ Uploading as **{mode_label}**…\n`{pending['filename']}`"
        )
    except Exception:
        pass

    await do_upload(
        client,
        callback_query.message,
        user_id,
        pending["url"],
        pending["filename"],
        force_document=(choice == "doc"),
        format_id=pending.get("format_id"),
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /upload <url>  — step 1: ask for rename
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("upload") & filters.private)
async def upload_handler(client: Client, message: Message):
    user = message.from_user
    await add_user(user.id, user.username)

    if await is_banned(user.id):
        return await message.reply_text("🚫 You are banned.")

    args = message.command
    
    # Check if bot upload is allowed or user is admin
    if not Config.ALLOW_BOT_URL_UPLOAD and user.id != Config.OWNER_ID and user.id not in Config.ADMIN:
        from pyrogram.types import WebAppInfo
        app_url = "https://swift-vilhelmina-akila-10dce4a8.koyeb.app"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📱 Open Web Interface 📱", web_app=WebAppInfo(url=app_url))]])
        return await message.reply_text(
            "⚠️ **Direct bot uploads are currently disabled.**\n\n"
            "Please use the **Web Interface** below to submit your links. It's faster and supports more features!",
            reply_markup=kb,
            quote=True
        )

    url = None
    if len(args) > 1:
        url = args[1].strip()
    elif message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()

    if not url or not url.startswith(("http://", "https://")):
        return await message.reply_text(
            "❌ Please provide a valid direct URL.\n\nUsage: `/upload https://example.com/file.mp4`",
            quote=True,
        )

    # Explicit YouTube Block
    if "youtube.com" in url.lower() or "youtu.be" in url.lower():
        return await message.reply_text("❌ YouTube downloading not allowed.", quote=True)

    status_info = await message.reply_text("🔍 Analyzing file info…", quote=True)
    try:
        url = await resolve_url(url)
    except Exception:
        pass

    # For yt-dlp URLs, fetch the video title to use as suggested filename
    if is_ytdlp_url(url):
        try:
            await status_info.edit_text("🔍 Fetching video info…")
        except Exception:
            pass
        fetched = await fetch_ytdlp_title(url)
        try:
            await status_info.delete()
        except Exception:
            pass
        orig_filename = fetched or smart_output_name(extract_filename(url))
    else:
        orig_filename = smart_output_name(extract_filename(url))
        try:
            await status_info.delete()
        except Exception:
            pass
    PENDING_RENAMES[user.id] = {"url": url, "orig": orig_filename}

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip (keep original)", callback_data=f"skip_rename:{user.id}")]
    ])
    await message.reply_text(
        f"✏️ **Rename file?**\n\n"
        f"📁 Original: `{orig_filename}`\n\n"
        "Send the **new filename** (with extension) or press **Skip**:",
        reply_markup=kb,
        quote=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /skip — keep original filename via command
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("skip") & filters.private)
async def skip_handler(client: Client, message: Message):
    user_id = message.from_user.id
    pending = PENDING_RENAMES.pop(user_id, None)
    if not pending:
        return await message.reply_text("❌ No pending upload. Send a URL first.", quote=True)

    prompt = await message.reply_text("⏭ Keeping original filename…", quote=True)
    await resolve_rename(client, prompt, user_id, pending["url"], pending["orig"])


# ─────────────────────────────────────────────────────────────────────────────
#  Text handler — rename input OR bare URL
# ─────────────────────────────────────────────────────────────────────────────

_ALL_COMMANDS = [
    "start", "help", "about", "upload", "skip", "caption", "showcaption",
    "clearcaption", "setthumb", "showthumb", "delthumb",
    "broadcast", "total", "ban", "unban", "status",
]


@Client.on_message(filters.private & filters.text & ~filters.command(_ALL_COMMANDS))
async def text_handler(client: Client, message: Message):
    user = message.from_user
    text = (message.text or "").strip()

    # ── Pending rename input ──────────────────────────────────────────────────
    if user.id in PENDING_RENAMES:
        pending = PENDING_RENAMES.pop(user.id)
        new_name = text.strip()
        # Preserve original extension if user didn't include one
        orig_ext = os.path.splitext(pending["orig"])[1]
        new_ext = os.path.splitext(new_name)[1]
        if not new_ext and orig_ext:
            new_name = new_name + orig_ext

        prompt = await message.reply_text(f"✏️ Renamed to: `{new_name}`", quote=True)
        await resolve_rename(client, prompt, user.id, pending["url"], new_name)
        return

    # ── Bare URL ──────────────────────────────────────────────────────────────
    if text.startswith(("http://", "https://")):
        await add_user(user.id, user.username)
        if await is_banned(user.id):
            return await message.reply_text("🚫 You are banned.")
        
        # Check if bot upload is allowed or user is admin
        if not Config.ALLOW_BOT_URL_UPLOAD and user.id != Config.OWNER_ID and user.id not in Config.ADMIN:
            from pyrogram.types import WebAppInfo
            app_url = "https://swift-vilhelmina-akila-10dce4a8.koyeb.app"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("📱 Open Web Interface 📱", web_app=WebAppInfo(url=app_url))]])
            return await message.reply_text(
                "⚠️ **Direct bot uploads are currently disabled.**\n\n"
                "Please use the **Web Interface** below to submit your links. It's faster and supports more features!",
                reply_markup=kb,
                quote=True
            )

        # Pre-flight check: Extract the real extension via HTTP Sniffing or yt-dlp first!
        
        # Explicit YouTube Block
        if "youtube.com" in text.lower() or "youtu.be" in text.lower():
            return await message.reply_text("❌ YouTube downloading not allowed.", quote=True)
            
        status_info = await message.reply_text("🔍 Analyzing file info…", quote=True)
        try:
            text = await resolve_url(text)
        except Exception:
            pass
        
        orig_filename = await get_best_filename(text)
        try:
            await status_info.delete()
        except Exception:
            pass
            
        PENDING_RENAMES[user.id] = {"url": text, "orig": orig_filename}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏭ Skip (keep original)", callback_data=f"skip_rename:{user.id}")]
        ])
        await message.reply_text(
            f"✏️ **Rename file?**\n\n"
            f"📁 Original: `{orig_filename}`\n\n"
            "Send the **new filename** (with extension) or press **Skip**:",
            reply_markup=kb,
            quote=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  Caption management
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("caption") & filters.private)
async def set_caption(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        return await message.reply_text("Usage: `/caption Your caption text here`", quote=True)
    caption = " ".join(args[1:])
    await update_user(message.from_user.id, {"caption": caption})
    await message.reply_text(f"✅ Caption saved:\n\n{caption}", quote=True)


@Client.on_message(filters.command("showcaption") & filters.private)
async def show_caption(client: Client, message: Message):
    user_data = await get_user(message.from_user.id) or {}
    cap = user_data.get("caption") or "_(none set)_"
    await message.reply_text(f"📝 Your caption:\n\n{cap}", quote=True)


@Client.on_message(filters.command("clearcaption") & filters.private)
async def clear_caption(client: Client, message: Message):
    await update_user(message.from_user.id, {"caption": ""})
    await message.reply_text("✅ Caption cleared.", quote=True)


# ─────────────────────────────────────────────────────────────────────────────
#  Thumbnail management — stored as Telegram file_id (permanent)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("setthumb") & filters.private)
async def set_thumb(client: Client, message: Message):
    reply = message.reply_to_message
    if not reply or not reply.photo:
        return await message.reply_text(
            "❌ Reply to a **photo** with /setthumb to save it as your thumbnail.",
            quote=True,
        )
    file_id = reply.photo.file_id
    await update_user(message.from_user.id, {"thumb": file_id})
    await message.reply_text(
        "✅ Thumbnail saved permanently!\n"
        "It will be applied to all your future uploads.",
        quote=True,
    )


@Client.on_message(filters.command("showthumb") & filters.private)
async def show_thumb(client: Client, message: Message):
    user_data = await get_user(message.from_user.id) or {}
    thumb_id = user_data.get("thumb")
    if not thumb_id:
        return await message.reply_text("❌ No thumbnail set. Reply to a photo with /setthumb.", quote=True)
    try:
        await message.reply_photo(photo=thumb_id, caption="🖼️ Your current thumbnail", quote=True)
    except Exception as e:
        await message.reply_text(f"❌ Could not show thumbnail: `{e}`", quote=True)


@Client.on_message(filters.command("delthumb") & filters.private)
async def del_thumb(client: Client, message: Message):
    await update_user(message.from_user.id, {"thumb": None})
    await message.reply_text("✅ Thumbnail deleted.", quote=True)

# ─────────────────────────────────────────────────────────────────────────────
#  WebApp Bridge Logic (Called by Flask)
# ─────────────────────────────────────────────────────────────────────────────

async def trigger_webapp_download(chat_id: int, url: str, format_id: str, mode: str, filename: str = None):
    """
    Called by Flask route `/api/download` to push a task directly into the bot queue.
    Because Flask handles the request synchronously, it fires this onto the main event loop.
    """
    # Initialize progress entry immediately so UI doesn't show "idle"
    WEBAPP_PROGRESS[chat_id] = {
        "action": "Initializing download...",
        "percentage": 1,
        "current": "0 B",
        "total": "Unknown",
        "speed": "---"
    }
    Config.LOGGER.info(f"Task queued for {chat_id}. SyncID={id(WEBAPP_PROGRESS)}")
    
    # We must quickly send an "Uploading..." stub to the user using the Pyrogram app
    status_msg = await bot_client.send_message(
        chat_id=chat_id,
        text=f"📥 **Web App Request Received:**\n`{url}`\n\n_Preparing download..._"
    )
    
    try:
        url = await resolve_url(url)
    except Exception:
        pass
        
    # Use provided filename or extract from URL
    if not filename:
        filename = extract_filename(url)
    filename = smart_output_name(filename)
    
    # Ensure it's not empty after sanitize
    if not filename or filename == ".":
        filename = "downloaded_file.bin"
    
    # Offload the heavy work onto `do_upload` logic
    # The mode string comes across as 'media' or 'doc'
    force_document = (mode == "doc")
    
    # Normalize "best_*" to "best" for unrestricted quality
    if format_id and format_id.startswith("best_"):
        format_id = "best"

    # Fire and forget onto the active loop
    asyncio.create_task(
        do_upload(
            bot_client,
            status_msg,  # Use the status_msg itself as the "reply_to" context
            chat_id,
            url,
            filename,
            force_document=force_document,
            format_id=format_id
        )
    )
