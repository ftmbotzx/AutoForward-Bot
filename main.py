import logging, asyncio, time, re
from pyrogram import Client, filters
from pyrogram import utils as pyroutils
from motor.motor_asyncio import AsyncIOMotorClient  # MongoDB (async)
import app  # ✅ Import our API module

# ✅ Pass main.py’s running loop to Flask
app.set_shared_loop(asyncio.get_event_loop())

# ✅ Start Flask API server
app.start_api_server()
# ✅ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# ✅ Fix peer ID ranges for large IDs
pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999

# ✅ MongoDB setup
MONGO_URI = "mongodb+srv://ftmbotzx:ftmbotzx@cluster0.0b8imks.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "forwaerDB"
COLLECTION_NAME = "progress1"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
progress_col = db[COLLECTION_NAME]

# ✅ Pyrogram Client (your string session)
app = Client(
    "forwarder",
    session_string="BQGb5dsANiWh9oVDNB4nQyOSDcWaAhkIglASHPoq5ssWc3JyLzDtqZDXJT1kWPjGXcYoYQoxtSeWxXqXGgGBmG1ss6h4FZ_3e4dxaF0F_hN5ScUWNDupxpFu98t6-X11VJdoawxJY5MUYPcLaLGTSdNOPEXzQiICRKmEK-a8Qj7OEkVcIMWNgQmS8_vqzWHZisZfn0mI_P1ZWzhPMDpGep7S8uYCtX2dRXlDk9R611hayRs0ngIiP142fULu3OH_IxN0nwgZ8hoxHSzY1gg387NyPXZCfbx0Zh3QpSOzE66c3hCBaHxSE3xeY6jc2AJdV3j7DLFDg600-Lgh1JPaP_8gYUJDZAAAAAGzDQ5BAA"
)

# ✅ Channels Config
SOURCE_CHANNEL = "Spotifyapk56"        # Source channel username (without @)
TARGET_CHANNEL = -1002762263047        # Target private channel ID
PROGRESS_CHANNEL = "@ftmdeveloperz"    # Log channel

# ✅ Stats tracking
stats = {"total_messages": 0, "forwarded": 0, "skipped": 0}
start_time = time.time()

# 🔍 Spotify Link Extractor


# ✅ MongoDB helpers
async def get_last_message_id():
    doc = await progress_col.find_one({"_id": "last_id"})
    return doc["message_id"] if doc else 0

async def save_last_message_id(msg_id):
    await progress_col.update_one(
        {"_id": "last_id"},
        {"$set": {"message_id": msg_id}},
        upsert=True
    )

# ✅ Manual progress bar (only triggered on /stats)
async def send_progress_bar():
    elapsed_minutes = max((time.time() - start_time) / 60, 1)
    speed = round(stats['forwarded'] / elapsed_minutes, 2)
    percentage = (stats['forwarded'] / stats['total_messages'] * 100) if stats['total_messages'] > 0 else 0

    text = f"""
╔════❰ ғᴏʀᴡᴀʀᴅ sᴛᴀᴛᴜs ❱═❍⊱❁
║┣⪼ ᴛᴏᴛᴀʟ: {stats['total_messages']}
║┣⪼ ғᴏʀᴡᴀʀᴅᴇᴅ: {stats['forwarded']}
║┣⪼ sᴋɪᴘᴘᴇᴅ: {stats['skipped']}
║┣⪼ sᴘᴇᴇᴅ: {speed} ᴍsɢs/ᴍɪɴ
║┣⪼ ᴘᴇʀᴄᴇɴᴛ: {round(percentage, 2)}%
╚════❰ ᴘʀᴏɢʀᴇssɪɴɢ ❱══❍⊱❁
"""
    try:
        await app.send_message(PROGRESS_CHANNEL, text)
        logging.info("📊 Progress bar sent.")
    except Exception as e:
        logging.error(f"⚠️ Could not send progress bar: {e}")





def extract_spotify_from_caption(caption):
    match = re.search(r'https?://open\.spotify\.com/track/([a-zA-Z0-9]+)', caption)
    if match:
        return {"track_id": match.group(1)}
    return {"track_id": None}


# ✅ Handle stats & ping in PROGRESS_CHANNEL
@app.on_message(filters.chat(PROGRESS_CHANNEL) & filters.text)
async def handle_commands(client, message):
    if message.text.lower() == "stats":
        await send_progress_bar()
    elif message.text.lower() == "ping":
        start = time.time()
        m = await message.reply_text("🏓 Pong...")
        end = time.time()
        await m.edit_text(f"🏓 Pong! `{round((end-start)*1000)}ms`")

# ✅ Main polling function
async def poll_channel():
    last_id = await get_last_message_id()
    logging.info(f"▶ Resuming from message ID: {last_id}")

    while True:
        try:
            new_last_id = last_id
            async for msg in app.get_chat_history(SOURCE_CHANNEL, limit=50):
                if msg.id <= last_id:
                    break  # Only process new messages

                stats["total_messages"] += 1
                try:
                    # ✅ Extract Spotify ID from caption
                    caption = msg.caption or ""
                    logging.info(f"New {caption}")
                    track_info = extract_spotify_from_caption(caption)
                    track_id = track_info.get("track_id") or "N/A"

                    # ✅ Get song & artist metadata
                    song_name = msg.audio.title if msg.audio and msg.audio.title else (msg.caption or "Unknown Title")
                    artist_name = msg.audio.performer if msg.audio and msg.audio.performer else "Unknown Artist"

                    # ✅ Build WhatsApp-style caption (ALWAYS shows ID)
                    caption_text = f"🎵 {song_name}\n👤 {artist_name}\n🆔 {track_id}"

                    # ✅ Forward message with new caption
                    if msg.audio:
                        await app.send_audio(TARGET_CHANNEL, audio=msg.audio.file_id, caption=caption_text)
                    elif msg.document:
                        await app.send_document(TARGET_CHANNEL, document=msg.document.file_id, caption=caption_text)
                    else:
                        await msg.copy(TARGET_CHANNEL)

                    stats["forwarded"] += 1
                    new_last_id = max(new_last_id, msg.id)
                    logging.info(f"✅ Forwarded message {msg.id}")

                except Exception as e:
                    stats["skipped"] += 1
                    logging.error(f"❌ Error forwarding message {msg.id}: {e}")
                    await app.send_message(PROGRESS_CHANNEL, f"⚠️ Forward error for `{msg.id}`\n{e}")

            # ✅ Save progress if new messages processed
            if new_last_id > last_id:
                await save_last_message_id(new_last_id)
                last_id = new_last_id

        except Exception as e:
            logging.error(f"⚠️ Polling error: {e}")
            await app.send_message(PROGRESS_CHANNEL, f"⚠️ Polling error: {e}")

        await asyncio.sleep(5)  # Poll every 5 sec

# ✅ Main function
async def main():
    await app.start()
    me = await app.get_me()
    logging.info(f"✅ Logged in as {me.first_name} ({me.id})")

    try:
        src_chat = await app.get_chat(SOURCE_CHANNEL)
        logging.info(f"📡 Source channel resolved: {src_chat.title} ({src_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve source channel: {e}")

    try:
        tgt_chat = await app.get_chat(TARGET_CHANNEL)
        logging.info(f"📡 Target channel resolved: {tgt_chat.title} ({tgt_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve target channel: {e}")

    await app.send_message(PROGRESS_CHANNEL, "🚀 **Forwarder Bot started (manual stats mode)**")

    asyncio.create_task(poll_channel())  # Start polling

    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
