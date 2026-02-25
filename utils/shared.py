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
    workers=40,              # Increased for high concurrency
    upload_boost=True,
    max_concurrent_transmissions=20, # Increased for multiple users
)

# Global HTTP session manager for connection pooling
HTTP_SESSION = None

async def get_http_session():
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        connector = asyncio.get_event_loop().create_task(
            # Large connection pool for serving thousands of users
            asyncio.to_thread(lambda: asyncio.run(asyncio.sleep(0))) # ensure loop is ready
        )
        import aiohttp
        # Limit to 100 connections per host, 1000 total
        connector = aiohttp.TCPConnector(limit=1000, limit_per_host=100)
        HTTP_SESSION = aiohttp.ClientSession(connector=connector)
    return HTTP_SESSION

async def close_http_session():
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

# Global dictionary for shared progress tracking between Flask and Pyrogram
WEBAPP_PROGRESS: dict[int, dict] = {}
