from flask import Flask

app = Flask(__name__)

# Set to True by bot.py once the Pyrogram client is connected to Telegram.
# Koyeb will only route traffic once /health returns 200.
app.is_ready = False

# Set to True when SIGTERM is received (graceful shutdown in progress).
app.is_shutting_down = False


@app.route("/")
def index():
    if app.is_shutting_down:
        return "🔄 Bot is shutting down…", 503
    if not app.is_ready:
        return "⏳ Bot is starting…", 503
    return "🤖 URL Uploader Bot is running!", 200


@app.route("/health")
def health():
    if app.is_shutting_down:
        return {"status": "shutting_down"}, 503
    if not app.is_ready:
        return {"status": "starting"}, 503
    return {"status": "ok"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
