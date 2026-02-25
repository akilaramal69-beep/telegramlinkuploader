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
        import aiohttp
        # Balanced limits: 500 total, 50 per host to prevent server bans
        connector = aiohttp.TCPConnector(
            limit=500, 
            limit_per_host=50,
            force_close=False, # Reuse connections
            enable_cleanup_closed=True
        )
        # Use a longer timeout for the session itself to handle slow probes
        timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
        HTTP_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return HTTP_SESSION

async def close_http_session():
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

# Global dictionary for shared progress tracking between Flask and Pyrogram
WEBAPP_PROGRESS: dict[int, dict] = {}
