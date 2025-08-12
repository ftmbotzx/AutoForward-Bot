import logging, asyncio, time, re
from telethon import TelegramClient, events, sync
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.sessions import StringSession
from motor.motor_asyncio import AsyncIOMotorClient  # MongoDB (async)
import app  # ✅ Import our API module

# ✅ Start Flask API server (will set loop later)
app.start_api_server()
# ✅ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# ✅ MongoDB setup
MONGO_URI = "mongodb+srv://ftmbotzx:ftmbotzx@cluster0.0b8imks.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "forwardDB"
COLLECTION_NAME = "ftmdb1"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
progress_col = db[COLLECTION_NAME]

# ✅ Telethon Client (your string session)
api_id = 8012239  # Replace with your API ID
api_hash = '171e6f1bf66ed8dcc5140fbe827b6b08'  # Replace with your API hash
session_string = "1BVtsOIUBu7dey3UyRGm88E_pF0u14z02H9u2zY9ZiO4KomI2HOQZzCO0n2U1wsAiMu7FRUebo88h3ZziYGGMK0AjlcS5yLfY-KJCtjhTUqIZJJHt0DAShLwj7PmsMNcACTvvU9FgbSC27Ijhy5WsBrxQ9nZIdmdTAtSNhNN-ihCdS440eAAFrrHVPQf6StoNbm6givDm6w48g2z6-6EkjzSS0Z_vbCIolITBRieTzw4_9DC4Do1Lrm_55r_Y6YXeBgfpedLI4C9LC_jE54uzRX-8LYe9Kp4FPe_0mu95ieLZJ1WG-WJHd6DFovDDE3r0gk-E_lWs2bJhQ-80gYxmS0cu48-MMXY=" # Replace with your string session
client = TelegramClient(StringSession(session_string), api_id, api_hash)


# ✅ Channels Config
SOURCE_CHANNEL = "Spotifyapk56"        # Source channel username (without @)
TARGET_CHANNEL = -1002752194267     # Target private channel ID
PROGRESS_CHANNEL = "@ftmdeveloperz"    # Log channel

# ✅ Stats tracking
stats = {"total_messages": 0, "forwarded": 0, "skipped": 0}
start_time = time.time()
session_start_id = None  # Track where this session started from

# 🔍 Spotify Link Extractor
def extract_spotify_from_msg(msg) -> dict:
    import re

    # Get raw text (caption or text)
    text = msg.message or ""
    logging.info(f"📝 Raw text: {repr(text)}")

    # Check entities (link formatting etc.)
    if hasattr(msg, 'entities') and msg.entities:
        for entity in msg.entities:
            if hasattr(entity, 'url') and entity.url:
                url = entity.url
                logging.info(f"🔗 Found text_link entity: {url}")
                match = re.search(r'https?://open\.spotify\.com/track/([a-zA-Z0-9]+)', url)
                if match:
                    return {"track_id": match.group(1)}

    # Fallback: look for visible URL in plain text
    match = re.search(r'https?://open\.spotify\.com/track/([a-zA-Z0-9]+)', text)
    if match:
        logging.info(f"✅ Spotify ID from plain text: {match.group(1)}")
        return {"track_id": match.group(1)}

    logging.warning("⚠️ No Spotify link found.")
    return {"track_id": None}


# ✅ MongoDB helpers
async def get_last_message_id():
    doc = await progress_col.find_one({"_id": "last_id"})
    return doc["message_id"] if doc else 18161900  # Default start point when DB is empty

async def save_last_message_id(msg_id):
    await progress_col.update_one(
        {"_id": "last_id"},
        {"$set": {"message_id": msg_id}},
        upsert=True
    )

async def check_database_status():
    """Check database connection and last message ID with message details"""
    try:
        doc = await progress_col.find_one({"_id": "last_id"})
        if doc:
            last_msg_id = doc['message_id']

            # Try to get details of the message from the source channel
            try:
                message = await client.get_messages(SOURCE_CHANNEL, ids=last_msg_id)
                if message:
                    msg_text = message.message[:50] + "..." if message.message and len(message.message) > 50 else (message.message or "No text")
                    msg_date = message.date.strftime("%Y-%m-%d %H:%M:%S") if message.date else "Unknown date"

                    session_info = f"\n🚀 Session started from: {session_start_id}\n📈 Progress this session: {last_msg_id - session_start_id} messages" if session_start_id else ""

                    return f"✅ Database connected.\n📍 Last processed: ID {last_msg_id}\n📝 Message: {msg_text}\n📅 Date: {msg_date}\n🔗 Link: https://t.me/{SOURCE_CHANNEL}/{last_msg_id}{session_info}"
                else:
                    return f"✅ Database connected.\n📍 Last message ID: {last_msg_id}\n⚠️ Message not found in channel"
            except Exception as e:
                return f"✅ Database connected.\n📍 Last message ID: {last_msg_id}\n❌ Could not fetch message details: {e}"
        else:
            return "⚠️ Database connected but no last_id found. Will start from default message ID 18161900."
    except Exception as e:
        return f"❌ Database error: {e}"

# ✅ Manual progress bar (only triggered on /stats)
async def send_progress_bar():
    elapsed_minutes = max((time.time() - start_time) / 60, 1)
    speed = round(stats['forwarded'] / elapsed_minutes, 2)
    percentage = (stats['forwarded'] / stats['total_messages'] * 100) if stats['total_messages'] > 0 else 0

    # Get current last processed message ID
    current_last_id = await get_last_message_id()

    text = f"""
╔════❰ ғᴏʀᴡᴀʀᴅ sᴛᴀᴛᴜs ❱═❍⊱❁
║┣⪼ sᴇssɪᴏɴ sᴛᴀʀᴛᴇᴅ ғʀᴏᴍ: {session_start_id}
║┣⪼ ᴄᴜʀʀᴇɴᴛ ʟᴀsᴛ ɪᴅ: {current_last_id}
║┣⪼ ᴛᴏᴛᴀʟ ᴄʜᴇᴄᴋᴇᴅ: {stats['total_messages']}
║┣⪼ ғᴏʀᴡᴀʀᴅᴇᴅ: {stats['forwarded']}
║┣⪼ sᴋɪᴘᴘᴇᴅ: {stats['skipped']}
║┣⪼ sᴘᴇᴇᴅ: {speed} ᴍsɢs/ᴍɪɴ
║┣⪼ ᴘʀᴏɢʀᴇss: {current_last_id - session_start_id} ᴍsɢs ᴘʀᴏᴄᴇssᴇᴅ
║┣⪼ ᴜᴘᴛɪᴍᴇ: {round(elapsed_minutes, 1)} ᴍɪɴᴜᴛᴇs
╚════❰ ᴘʀᴏɢʀᴇssɪɴɢ ❱══❍⊱❁
"""
    try:
        await client.send_message(PROGRESS_CHANNEL, text)
        logging.info("📊 Progress bar sent.")
    except Exception as e:
        logging.error(f"⚠️ Could not send progress bar: {e}")



#ftmdev
@client.on(events.NewMessage(chats=PROGRESS_CHANNEL, pattern=r'^!(stats|ping|db)$'))
async def handle_commands(event):
    command = event.pattern_match.group(1).lower()
    logging.info(f"🔧 Command received: {command}")

    if command == "stats":
        await send_progress_bar()
    elif command == "ping":
        start = time.time()
        m = await event.reply("🏓 Pong...")
        end = time.time()
        await m.edit(f"🏓 Pong! `{round((end-start)*1000)}ms`")
    elif command == "db":
        db_status = await check_database_status()
        await event.reply(db_status)

# ✅ Main polling function
async def poll_channel():
    global session_start_id
    last_id = await get_last_message_id()
    session_start_id = last_id  # Track where this session started

    logging.info(f"▶ Resuming from message ID: {last_id}")

    # Send session start notification
    try:
        start_msg = await client.get_messages(SOURCE_CHANNEL, ids=last_id)
        if start_msg:
            await client.send_message(PROGRESS_CHANNEL, 
                f"🚀 **Session Started**\n"
                f"📍 Starting from: ID {last_id}\n"
                f"🔗 Link: https://t.me/{SOURCE_CHANNEL}/{last_id}\n"
                f"⏰ Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        logging.error(f"Could not send session start notification: {e}")

    while True:
        try:
            new_last_id = last_id
            async for msg in client.iter_messages(SOURCE_CHANNEL, limit=50):
                if msg.id <= last_id:
                    break  # Only process new messages

                stats["total_messages"] += 1
                try:
                    # ✅ Extract Spotify ID from caption
                    track_info = extract_spotify_from_msg(msg)
                    track_id = track_info.get("track_id") or "N/A"

                    # ✅ Get song & artist metadata
                    song_name = "Unknown Title"
                    artist_name = "Unknown Artist"

                    if hasattr(msg, 'media') and msg.media:
                        if hasattr(msg.media, 'document') and msg.media.document:
                            for attr in msg.media.document.attributes:
                                if hasattr(attr, 'title') and attr.title:
                                    song_name = attr.title
                                if hasattr(attr, 'performer') and attr.performer:
                                    artist_name = attr.performer

                    if song_name == "Unknown Title" and msg.message:
                        song_name = msg.message

                    # ✅ Build WhatsApp-style caption (ALWAYS shows ID)
                    caption_text = f"🎵 {song_name}\n👤 {artist_name}\n🆔 {track_id}"

                    # ✅ Forward message with new caption
                    if hasattr(msg, 'media') and msg.media:
                        await client.send_file(TARGET_CHANNEL, msg.media, caption=caption_text)
                    else:
                        await client.forward_messages(TARGET_CHANNEL, msg)

                    stats["forwarded"] += 1
                    new_last_id = max(new_last_id, msg.id)
                    logging.info(f"✅ Forwarded message {msg.id}")

                except Exception as e:
                    stats["skipped"] += 1
                    logging.error(f"❌ Error forwarding message {msg.id}: {e}")
                    await client.send_message(PROGRESS_CHANNEL, f"⚠️ Forward error for `{msg.id}`\n{e}")

            # ✅ Save progress if new messages processed
            if new_last_id > last_id:
                await save_last_message_id(new_last_id)
                last_id = new_last_id

        except Exception as e:
            logging.error(f"⚠️ Polling error: {e}")
            await client.send_message(PROGRESS_CHANNEL, f"⚠️ Polling error: {e}")

        await asyncio.sleep(5)  # Poll every 5 sec

# ✅ Main function
async def main():
    await client.start()
    me = await client.get_me()
    logging.info(f"✅ Logged in as {me.first_name} ({me.id})")

    try:
        src_chat = await client.get_entity(SOURCE_CHANNEL)
        logging.info(f"📡 Source channel resolved: {src_chat.title} ({src_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve source channel: {e}")

    try:
        tgt_chat = await client.get_entity(TARGET_CHANNEL)
        logging.info(f"📡 Target channel resolved: {tgt_chat.title} ({tgt_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve target channel: {e}")

    await client.send_message(PROGRESS_CHANNEL, "🚀 **Forwarder Bot started (manual stats mode)**")

    asyncio.create_task(poll_channel())  # Start polling

    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
