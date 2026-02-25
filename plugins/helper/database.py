import motor.motor_asyncio
from plugins.config import Config

_client = None
_db = None


def get_db():
    global _client, _db
    if _db is None and Config.DATABASE_URL:
        _client = motor.motor_asyncio.AsyncIOMotorClient(Config.DATABASE_URL)
        _db = _client["url_uploader"]
    return _db


async def add_user(user_id: int, username: str | None = None) -> None:
    db = get_db()
    if db is None:
        return
    await db.users.update_one(
        {"_id": user_id},
        {"$setOnInsert": {"_id": user_id, "username": username, "banned": False, "caption": "", "thumb": None}},
        upsert=True,
    )


async def get_user(user_id: int) -> dict | None:
    db = get_db()
    if db is None:
        return None
    return await db.users.find_one({"_id": user_id})


async def update_user(user_id: int, data: dict) -> None:
    db = get_db()
    if db is None:
        return
    await db.users.update_one({"_id": user_id}, {"$set": data}, upsert=True)


async def get_all_users() -> list[dict]:
    db = get_db()
    if db is None:
        return []
    return await db.users.find({}).to_list(length=None)


async def total_users_count() -> int:
    db = get_db()
    if db is None:
        return 0
    return await db.users.count_documents({})


async def is_banned(user_id: int) -> bool:
    user = await get_user(user_id)
    return bool(user and user.get("banned"))


async def ban_user(user_id: int) -> None:
    await update_user(user_id, {"banned": True})


async def unban_user(user_id: int) -> None:
    await update_user(user_id, {"banned": False})
