import os
import subprocess
import sys
import threading
import asyncio
from plugins.config import Config
from pyrogram import Client, idle, filters
import app  # noqa: F401

from utils.shared import bot_client, WEBAPP_PROGRESS

# Register a global ping handler for diagnostics
@bot_client.on_message(filters.command("ping") & filters.private)
async def ping_handler(client, message):
    print(f"📥 Received /ping from {message.from_user.id} at {time.time()}")
    await message.reply_text("🏓 Pong! Bot is alive and well.")
def run_health_server():
    from app import app as flask_app
    from waitress import serve
    print("🌍 Starting health & progress server with Waitress (Production)...")
    serve(flask_app, host="0.0.0.0", port=8080, threads=100)


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀  URL Uploader Bot — Starting…")
    print("=" * 60 + "\n")

    # ── Validate required environment variables ──────────────────────────
    missing = []
    if not Config.BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not Config.API_ID:
        missing.append("API_ID")
    if not Config.API_HASH:
        missing.append("API_HASH")
    if missing:
        print(f"❌ FATAL: Missing required environment variables: {', '.join(missing)}")
        print("   Set them in .env or in your Koyeb environment settings.")
        sys.exit(1)

    # Ensure download folder exists and is clean on startup
    if os.path.exists(Config.DOWNLOAD_LOCATION):
        import shutil
        try:
            shutil.rmtree(Config.DOWNLOAD_LOCATION)
            print("🧹 Cleaned old DOWNLOADS folder on startup.")
        except Exception as e:
            print(f"⚠️ Could not clean DOWNLOADS folder: {e}")
    os.makedirs(Config.DOWNLOAD_LOCATION, exist_ok=True)

    # Handle cookies from environment variable (useful for Koyeb)
    # Koyeb env vars may store newlines as literal \n — convert them
    cookies_data = os.environ.get("COOKIES_DATA", "")
    if cookies_data:
        cookies_data = cookies_data.replace("\\n", "\n")
        try:
            with open(Config.COOKIES_FILE, "w", encoding="utf-8") as f:
                f.write(cookies_data)
            print(f"🍪 Cookies written to {Config.COOKIES_FILE} from COOKIES_DATA env var.")
        except Exception as e:
            print(f"❌ Failed to write cookies file: {e}")

    print("🚀 Starting aria2c RPC daemon...")
    try:
        aria_cmd = [
            "aria2c",
            "--enable-rpc",
            "--rpc-listen-all=true",
            "--rpc-allow-origin-all=true",
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=1M",
            "--max-overall-download-limit=0",
            "--file-allocation=none",
            "--max-concurrent-downloads=100",
            "-D"
        ]
        subprocess.Popen(aria_cmd)
        print("✅ aria2c daemon started with optimized high-concurrency flags.")
    except Exception as e:
        print(f"⚠️ Failed to start aria2c daemon: {e}")

    # Start Flask health server in background thread (required by Koyeb)
    # Health check returns 503 until bot is fully connected (see app.py)
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    print("🌐 Health server started on port 8080 (returning 503 until bot is ready)")

    # ── Lifecycle: start → mark healthy → idle → shutdown ────────────────
    async def main():
        print("🔧 Initializing main coroutine...")
        # Try to connect and verify identity
        print("🔗 Connecting bot client...")
        await bot_client.start()
        print("✅ Bot client started.")
        
        try:
            me = await bot_client.get_me()
            print(f"✅ Logged in as: @{me.username}")
        except Exception as e:
            print(f"⚠️ Could not get bot info: {e}")

        # Capture the active asyncio loop so Flask threads can dispatch tasks to it
        print("🌀 Capturing event loop...")
        from app import app as flask_app, prune_progress_task
        flask_app.bot_loop = asyncio.get_running_loop()

        # Start the background pruning task
        asyncio.create_task(prune_progress_task())
        print("🧹 Progress pruning task started.")

        # Mark health check as ready — Koyeb now routes traffic here
        flask_app.is_ready = True
        print("🎊 BOT IS ALIVE 🎊 (health check → 200)")

        # Use Pyrogram's own idle() — handles SIGTERM/SIGINT properly
        await idle()

        # Signal received — mark as shutting down
        print("👋 Bot stopping cleanly. Goodbye!")
        await bot_client.stop()

    # Run everything manually since we want more control over start/stop
    print("🎬 Starting event loop...")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except Exception as e:
        print(f"❌ Bot crashed: {e}")
        import traceback
        traceback.print_exc()
