import os
import asyncio
from flask import Flask, request, jsonify, send_from_directory
from plugins.config import Config
import time

# Serve the MiniApp directly from the new `web/` folder
app = Flask(__name__, static_folder="web")

# Runtime flags used by bot.py
app.is_ready = False
app.is_shutting_down = False

# Global cache for the optimized HTML to save Disk I/O
_INDEX_HTML_CACHE = None

async def prune_progress_task():
    """Background task to keep memory low by pruning old progress data."""
    from utils.shared import WEBAPP_PROGRESS
    while True:
        try:
            now = time.time()
            # Remove entries that haven't been updated for 1 hour
            to_del = [uid for uid, info in WEBAPP_PROGRESS.items() 
                      if now - info.get("_last_update", now) > 3600]
            for uid in to_del:
                del WEBAPP_PROGRESS[uid]
        except Exception:
            pass
        await asyncio.sleep(600) # Check every 10 mins

@app.route("/")
def index():
    global _INDEX_HTML_CACHE
    if app.is_shutting_down:
        return "🔄 Bot is shutting down…", 503
    if not app.is_ready:
        return "⏳ Bot is starting…", 503

    if _INDEX_HTML_CACHE:
        return _INDEX_HTML_CACHE

    try:
        html_path = os.path.join("web", "index.html")
        if not os.path.exists(html_path):
            return "404 - Web assets missing", 404
            
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
            # Inject Block ID directly into HTML to save an API request
            content = content.replace("{{ADSGRAM_BLOCK_ID}}", Config.ADSGRAM_BLOCK_ID)
            _INDEX_HTML_CACHE = content
            return content
    except Exception as e:
        Config.LOGGER.error(f"Error serving index: {e}")
        return "Internal Server Error", 500

@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory("web", path)


@app.route('/api/config', methods=['GET'])
def api_config():
    """Return public configuration values to the frontend."""
    return jsonify({
        "adsgram_block_id": Config.ADSGRAM_BLOCK_ID
    }), 200

@app.route("/api/formats", methods=["POST"])
def api_formats():
    """Endpoint for MiniApp to extract video qualities without uploading."""
    if not app.is_ready:
        return {"error": "Bot is not ready"}, 503

    data = request.json
    url = data.get("url")
    if not url:
        return {"error": "No URL provided"}, 400

    from plugins.helper.upload import fetch_ytdlp_formats
    
    # Needs to spawn in loop since Flask is synchronous here
    try:
        # We must push this coroutine onto the main Pyrogram loop instead of making a new one
        future = asyncio.run_coroutine_threadsafe(fetch_ytdlp_formats(url), app.bot_loop)
        res = future.result(timeout=60)
        return jsonify(res), 200
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/download", methods=["POST"])
def api_download():
    """Triggered when user clicks 'Beam to Chat' in the MiniApp"""
    if not app.is_ready:
        return {"error": "Bot is not ready"}, 503

    data = request.json
    url = data.get("url")
    if not url or not data.get("chat_id"):
        return {"error": "URL or chat_id missing."}, 400

    chat_id = int(data.get("chat_id"))
    format_id = data.get("format_id")
    mode = data.get("mode", "media")
    filename = data.get("filename")

    from plugins.commands import trigger_webapp_download
    
    # We must quickly queue this task onto Pyrogram's async loop
    try:
         asyncio.run_coroutine_threadsafe(trigger_webapp_download(chat_id, url, format_id, mode, filename), app.bot_loop)
         return jsonify({"status": "queued"}), 200
    except Exception as e:
         return {"error": str(e)}, 500

@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    if not app.is_ready:
        return {"error": "Bot is not ready"}, 503

    data = request.json
    user_id = data.get("user_id")
    if not user_id:
        return {"error": "user_id missing."}, 400

    user_id = int(user_id)
    from plugins.commands import ACTIVE_TASKS
    task_info = ACTIVE_TASKS.get(user_id)
    if not task_info:
        return {"error": "No active process to cancel."}, 404

    task, cancel_ref = task_info
    cancel_ref[0] = True
    
    # Safely cancel the task on the bot's async loop
    app.bot_loop.call_soon_threadsafe(task.cancel)

    return jsonify({"status": "cancelled"}), 200

@app.route("/api/progress", methods=["GET"])
def api_progress():
    """Endpoint for MiniApp to poll live download/upload progress."""
    if not app.is_ready:
        return {"error": "Bot is not ready"}, 503

    user_id_str = request.args.get("user_id")
    if not user_id_str or not user_id_str.isdigit():
        return {"error": "Invalid user_id."}, 400

    user_id = int(user_id_str)
    from plugins.config import Config
    from utils.shared import WEBAPP_PROGRESS
    
    progress_data = WEBAPP_PROGRESS.get(user_id)
    # Safe logging
    Config.LOGGER.info(f"Progress request for {user_id}: SyncID={id(WEBAPP_PROGRESS)}, Found={bool(progress_data)}")
    
    if progress_data:
        return jsonify(progress_data), 200
    else:
        return jsonify({"action": "idle", "percentage": 0}), 200

@app.route("/api/debug_state")
def api_debug_state():
    from utils.shared import WEBAPP_PROGRESS
    import sys
    res = {
        "sync_id": id(WEBAPP_PROGRESS),
        "keys": list(WEBAPP_PROGRESS.keys()),
        "data": WEBAPP_PROGRESS,
        "python_path": sys.path
    }
    return jsonify(res), 200

@app.route("/health")
def health():
    if app.is_shutting_down:
        return {"status": "shutting_down"}, 503
    if not app.is_ready:
        return {"status": "starting"}, 503
    return {"status": "ok"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
