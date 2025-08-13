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
session_string = "1BVtsOIsBuz87kY2XOsjO2cVFnHJQoweOxT1UHxPcMD0INENo4rIyVNWmut-GUozCk0m5--J9HehX7Vg_cvPWrapE9x5hbSyfBVPRAJSAB2Y1iVNJGjyAvpNCWYumzG2Np4adB-AUKboLFjjWTKsKS9r8NBEes3mdDNdpV63LICOGMnqfSjJ2DYd51luISvVFU-D61GgaL3Ig8z4Pl05qx7eoYrBumtCJKqPjpSEQpuP5S4Ch1QVMFozp8FjpQkp4XnTaNXkOjH64FTU-GCQRcaSqKUFfIaXJEtrH_sbC06osxHuk4OoDh4v0cDV_7ASWoW11KvTBM7uc2IOJ8-6OM4SzXukh0MU=" # Replace with your string session
client = TelegramClient(StringSession(session_string), api_id, api_hash)


# ✅ Channels Config
SOURCE_CHANNEL = "Spotifyapk56"        # Source channel username (without @)
TARGET_CHANNEL = -1002752194267     # Target private channel ID
PROGRESS_CHANNEL = "@ftmdeveloperz"    # Log channel

# ✅ Stats tracking
stats = {"total_messages": 0, "forwarded": 0, "skipped": 0, "duplicates": 0, "errors": 0}
start_time = time.time()
session_start_id = None  # Track where this session started from
is_paused = False  # Bot pause/resume control
forwarding_speed = 5  # Delay between messages in seconds

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
        {"$set": {"message_id": msg_id, "timestamp": time.time()}},
        upsert=True
    )

async def save_message_record(msg_id, track_id, song_name, artist_name, status="forwarded"):
    """Save individual message record to database"""
    await progress_col.update_one(
        {"_id": f"msg_{msg_id}"},
        {"$set": {
            "message_id": msg_id,
            "track_id": track_id,
            "song_name": song_name,
            "artist_name": artist_name,
            "status": status,
            "timestamp": time.time(),
            "date": time.strftime('%Y-%m-%d %H:%M:%S')
        }},
        upsert=True
    )

async def get_database_report():
    """Generate comprehensive database report"""
    try:
        # Get last processed message
        last_doc = await progress_col.find_one({"_id": "last_id"})
        last_id = last_doc.get("message_id", 18246934) if last_doc else 18246934
        
        # Count different types of records
        total_messages = await progress_col.count_documents({"_id": {"$regex": "^msg_"}})
        forwarded_count = await progress_col.count_documents({"_id": {"$regex": "^msg_"}, "status": "forwarded"})
        error_count = await progress_col.count_documents({"_id": {"$regex": "^msg_"}, "status": "error"})
        
        # Get recent messages (last 10)
        recent_cursor = progress_col.find({"_id": {"$regex": "^msg_"}}).sort("timestamp", -1).limit(10)
        recent_messages = []
        async for msg in recent_cursor:
            recent_messages.append({
                "id": msg["message_id"],
                "song": msg.get("song_name", "Unknown"),
                "artist": msg.get("artist_name", "Unknown"), 
                "track_id": msg.get("track_id", "N/A"),
                "status": msg.get("status", "unknown"),
                "date": msg.get("date", "Unknown")
            })
        
        # Calculate session stats
        session_processed = last_id - session_start_id if session_start_id else 0
        uptime_hours = (time.time() - start_time) / 3600
        
        return {
            "current_last_id": last_id,
            "session_start_id": session_start_id,
            "session_processed": session_processed,
            "total_in_db": total_messages,
            "forwarded": forwarded_count,
            "errors": error_count,
            "recent_messages": recent_messages,
            "uptime_hours": round(uptime_hours, 2),
            "stats": stats
        }
    except Exception as e:
        return {"error": f"Database report error: {e}"}

async def restart_from_message_id(target_msg_id):
    """Restart forwarding from a specific message ID"""
    try:
        await save_last_message_id(target_msg_id - 1)  # Set to one before target so we start from target
        global session_start_id
        session_start_id = target_msg_id
        logging.info(f"🔄 Restarted forwarding from message ID: {target_msg_id}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to restart from message ID {target_msg_id}: {e}")
        return False

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
@client.on(events.NewMessage(chats=PROGRESS_CHANNEL, pattern=r'^!(stats|ping|db|restart)(?:\s+(\d+))?$'))
async def handle_commands(event):
    command = event.pattern_match.group(1).lower()
    arg = event.pattern_match.group(2)
    logging.info(f"🔧 Command received: {command} {arg or ''}")

    if command == "stats":
        await send_progress_bar()
    elif command == "ping":
        start = time.time()
        m = await event.reply("🏓 Pong...")
        end = time.time()
        await m.edit(f"🏓 Pong! `{round((end-start)*1000)}ms`")
    elif command == "db":
        report = await get_database_report()
        if "error" in report:
            await event.reply(f"❌ {report['error']}")
        else:
            recent_msgs = "\n".join([
                f"• ID {msg['id']}: {msg['song'][:30]}... - {msg['status']}"
                for msg in report['recent_messages'][:5]
            ])
            
            db_report = f"""
╔════❰ ᴅᴀᴛᴀʙᴀsᴇ ʀᴇᴘᴏʀᴛ ❱═❍⊱❁
║┣⪼ ᴄᴜʀʀᴇɴᴛ ʟᴀsᴛ ɪᴅ: {report['current_last_id']}
║┣⪼ sᴇssɪᴏɴ sᴛᴀʀᴛ: {report['session_start_id']}
║┣⪼ sᴇssɪᴏɴ ᴘʀᴏᴄᴇssᴇᴅ: {report['session_processed']}
║┣⪼ ᴛᴏᴛᴀʟ ɪɴ ᴅʙ: {report['total_in_db']}
║┣⪼ ғᴏʀᴡᴀʀᴅᴇᴅ: {report['forwarded']}
║┣⪼ ᴇʀʀᴏʀs: {report['errors']}
║┣⪼ ᴜᴘᴛɪᴍᴇ: {report['uptime_hours']}ʜ
║┣⪼ ʟɪᴠᴇ sᴛᴀᴛs: ᴛ:{report['stats']['total_messages']} ғ:{report['stats']['forwarded']} s:{report['stats']['skipped']}
║
║📋 ʀᴇᴄᴇɴᴛ ᴍᴇssᴀɢᴇs:
{recent_msgs}
╚════❰ ᴄᴏᴍᴘʟᴇᴛᴇ ❱══❍⊱❁
"""
            await event.reply(db_report)
    elif command == "restart":
        if arg and arg.isdigit():
            target_id = int(arg)
            success = await restart_from_message_id(target_id)
            if success:
                await event.reply(f"✅ Restarted forwarding from message ID: {target_id}")
            else:
                await event.reply(f"❌ Failed to restart from message ID: {target_id}")
        else:
            await event.reply("❌ Usage: !restart <message_id>\nExample: !restart 18246934")

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

                # ✅ Check if bot is paused
                if is_paused:
                    await asyncio.sleep(1)
                    continue
                    
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

                    # ✅ Check if source is Spotifyapk56 or similar music channel that needs caption formatting
                    source_str = str(SOURCE_CHANNEL).lower()
                    is_spotify_channel = ('spotifyapk' in source_str or 
                                        'spotify' in source_str or 
                                        SOURCE_CHANNEL == 'Spotifyapk56' or 
                                        SOURCE_CHANNEL == '@Spotifyapk56')
                    
                    if is_spotify_channel:
                        # ✅ Build enhanced caption for Spotify channel with track ID
                        file_size_mb = msg.media.document.size // (1024*1024) if hasattr(msg, 'media') and msg.media and hasattr(msg.media, 'document') else 0
                        spotify_link = f"https://open.spotify.com/track/{track_id}" if track_id != "N/A" else ""
                        
                        caption_parts = [f"🎵 {song_name}", f"👤 {artist_name}"]
                        
                        if track_id != "N/A":
                            caption_parts.append(f"🆔 {track_id}")
                        caption_text = "\n".join(caption_parts)
                    else:
                        # ✅ For other channels, use original caption as-is
                        caption_text = msg.message if msg.message else None

                    # ✅ Forward message with new caption
                    if hasattr(msg, 'media') and msg.media:
                        await client.send_file(TARGET_CHANNEL, msg.media, caption=caption_text)
                    else:
                        await client.forward_messages(TARGET_CHANNEL, msg)

                    # ✅ Save message record to database
                    await save_message_record(msg.id, track_id, song_name, artist_name, "forwarded")
                    
                    stats["forwarded"] += 1
                    new_last_id = max(new_last_id, msg.id)
                    logging.info(f"✅ Forwarded message {msg.id}")

                    # ✅ Apply forwarding speed delay
                    await asyncio.sleep(forwarding_speed)

                except Exception as e:
                    stats["skipped"] += 1
                    stats["errors"] += 1
                    
                    # ✅ Save error record to database
                    await save_message_record(msg.id, "N/A", "Error", "Error", "error")
                    
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
    # ✅ Set shared loop for Flask API
    loop = asyncio.get_event_loop()
    app.set_shared_loop(loop)
    
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

    # ✅ Initialize from specific message ID (18246934)
    current_last_id = await get_last_message_id()
    if current_last_id < 18246934:
        await restart_from_message_id(18246934)
        logging.info("🔄 Initialized starting point to message ID: 18246934")

    await client.send_message(PROGRESS_CHANNEL, 
        "🚀 **Forwarder Bot started**\n"
        f"📍 Ready to forward from: https://t.me/{SOURCE_CHANNEL}/18246934\n"
        "🔧 Commands: !stats !db !ping !restart <id>")

    asyncio.create_task(poll_channel())  # Start polling

    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
