import threading
import time
from flask import Flask, jsonify
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

# --- CONFIG ---
MONGO_URI = "mongodb+srv://ftmbotzx:ftmbotzx@cluster0.0b8imks.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "ftmdb"
PROGRESS_COLLECTION = "ftmdb"
FORWARDED_COLLECTION = "forwarded_files"

# --- Flask App ---
app = Flask(__name__)

# --- MongoDB Connection ---
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
progress_col = db[PROGRESS_COLLECTION]
forwarded_col = db[FORWARDED_COLLECTION]

# ✅ We'll inject the running loop from main.py
SHARED_LOOP: asyncio.AbstractEventLoop = None  


def run_async(coro):
    """Safely run async coroutines from Flask using the shared loop."""
    if SHARED_LOOP is None:
        raise RuntimeError("Shared loop is not set. Call set_shared_loop(loop) from main.py.")
    return asyncio.run_coroutine_threadsafe(coro, SHARED_LOOP).result()


@app.route("/")
def home():
    """API root endpoint."""
    return jsonify({"status": "ok", "message": "Forwarder API is running"})


@app.route("/stats")
def get_stats():
    """Get current stats (total, forwarded, skipped)."""
    async def fetch_stats():
        # Get last message ID
        last_id_doc = await progress_col.find_one({"_id": "last_id"})
        last_id = last_id_doc.get("message_id", 18161900) if last_id_doc else 18161900
        
        # Import stats from main.py if available
        try:
            import main
            current_stats = main.stats.copy()
            current_stats["last_message_id"] = last_id
            current_stats["session_start_id"] = main.session_start_id
            current_stats["uptime_minutes"] = round((time.time() - main.start_time) / 60, 1)
            return current_stats
        except:
            return {
                "total_messages": 0,
                "forwarded": 0,
                "skipped": 0,
                "last_message_id": last_id,
                "last_updated": int(time.time())
            }

    stats = run_async(fetch_stats())
    return jsonify(stats)


@app.route("/files")
def get_files():
    """Get last 50 forwarded files info."""
    async def fetch_files():
        files = []
        cursor = forwarded_col.find().sort("time", -1).limit(50)
        async for f in cursor:
            files.append({
                "song_name": f.get("song_name", "Unknown"),
                "artist": f.get("artist", "Unknown"),
                "track_id": f.get("track_id", "N/A"),
                "forwarded_at": f.get("time", 0)
            })
        return files

    files = run_async(fetch_files())
    return jsonify(files)


@app.route("/progress")
def get_progress():
    """Get last forwarded message ID for auto-resume."""
    async def fetch_progress():
        return await progress_col.find_one({"_id": "last_id"}) or {"message_id": 18161900}

    progress = run_async(fetch_progress())
    return jsonify(progress)


def set_shared_loop(loop: asyncio.AbstractEventLoop):
    """Main.py will call this to give Flask the shared asyncio loop."""
    global SHARED_LOOP
    SHARED_LOOP = loop


def run_flask():
    """Run Flask app in a separate thread."""
    app.run(host="0.0.0.0", port=5000)


def start_api_server():
    """Launch Flask in a thread (so main.py can run together)."""
    thread = threading.Thread(target=run_flask)
    thread.daemon = True
    thread.start()


if __name__ == "__main__":
    run_flask()
