import os
import sys
import threading
from plugins.config import Config
from pyrogram import Client, idle


def run_health_server():
    import app  # noqa: F401 – registers routes
    from app import app as flask_app
    flask_app.run(host="0.0.0.0", port=8080, use_reloader=False)


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

    # Ensure download folder exists
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

    # Start Flask health server in background thread (required by Koyeb)
    # Health check returns 503 until bot is fully connected (see app.py)
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    print("🌐 Health server started on port 8080 (returning 503 until bot is ready)")

    # ── Build bot ────────────────────────────────────────────────────────
    plugins = dict(root="plugins")
    bot = Client(
        Config.SESSION_NAME,
        bot_token=Config.BOT_TOKEN,
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        plugins=plugins,
        sleep_threshold=300,
        workers=8,
        upload_boost=True,
        max_concurrent_transmissions=5,
    )

    # ── Lifecycle: start → mark healthy → idle → shutdown ────────────────
    async def main():
        from app import app as flask_app

        await bot.start()
        print("✅ Bot connected to Telegram")

        # Mark health check as ready — Koyeb now routes traffic here
        flask_app.is_ready = True
        print("🎊 BOT IS ALIVE 🎊 (health check → 200)")

        # Use Pyrogram's own idle() — handles SIGTERM/SIGINT properly
        await idle()

        # Signal received — mark as shutting down
        print("⚠️  Shutdown signal received — stopping bot…")
        flask_app.is_ready = False
        flask_app.is_shutting_down = True

        await bot.stop()
        print("👋 Bot stopped cleanly. Goodbye!")

    # bot.run() uses Pyrogram's own event loop management
    bot.run(main())
