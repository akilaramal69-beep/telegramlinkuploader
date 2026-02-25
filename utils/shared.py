import asyncio
from pyrogram import Client
from plugins.config import Config
import time

print(f"🧬 Loading utils.shared at {time.time()} (Memory ID: {id(Config)})")

# Initialize the Bot Client here so it can be safely imported by any module
# without causing circular imports or re-running the entry-point script.
plugins = dict(root="plugins")

bot_client = Client(
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

# Global dictionary for shared progress tracking between Flask and Pyrogram
WEBAPP_PROGRESS: dict[int, dict] = {}
