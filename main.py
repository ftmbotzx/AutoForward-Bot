import logging
import asyncio
import time
import re
import os
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from motor.motor_asyncio import AsyncIOMotorClient
import app  # Flask API module

# Start Flask API server
app.start_api_server()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Suppress Flask/Werkzeug logs to reduce noise
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('flask').setLevel(logging.ERROR)

# Create a specific logger for the main bot
bot_logger = logging.getLogger('main_bot')
bot_logger.setLevel(logging.INFO)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://ftmeditron:ftm@cluster0.plyrl7d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = "ftmdb"
COLLECTION_NAME = "ftmdb"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]
progress_col = db[COLLECTION_NAME]

# Telethon Client setup
api_id = int(os.getenv("API_ID", "8012239"))
api_hash = os.getenv("API_HASH", "171e6f1bf66ed8dcc5140fbe827b6b08")
session_string = os.getenv("SESSION_STRING", "df9cbs5Lk1xq2OuLF4uEgXTwulRRrUXlVY0SaSy6T4LZcRPB1EEYLSRDl1-i0Yw8Pg22C8ktSVzGZHovzt2rh51C7BUhv1QaNYqjVTVtDDPo6HO71qyQ2MGOlU7s-gadC_9VcqhrfAgHbRMvuKNnz-2lh2ESuWCxtuTwThHVBZdbSwrLatCyBBc4cBR1ZEkYl2-qUEQckVjQ=")

client = TelegramClient(StringSession(session_string), api_id, api_hash)

# Channels Config
SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL", "Spotifyapk56")
TARGET_CHANNEL = int(os.getenv("TARGET_CHANNEL", "-1002752194267"))
PROGRESS_CHANNEL = os.getenv("PROGRESS_CHANNEL", "@ftmdeveloperz")

# Stats tracking
stats = {"total_messages": 0, "forwarded": 0, "skipped": 0, "duplicates": 0, "errors": 0}
start_time = time.time()
session_start_id = None
is_paused = False
forwarding_speed = int(os.getenv("FORWARDING_SPEED", "3"))  # Delay between messages
is_catch_up_complete = False  # Track if we're in catch-up or live mode

# Spotify Link Extractor - keeping original logic
def extract_spotify_from_msg(msg) -> dict:
    """Extract Spotify track ID from message text or entities"""
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

# MongoDB helpers
async def get_last_message_id():
    """Get the last processed message ID from database"""
    doc = await progress_col.find_one({"_id": "last_id"})
    return doc["message_id"] if doc else 18246934

async def save_last_message_id(msg_id):
    """Save the last processed message ID to database"""
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
        global session_start_id, is_catch_up_complete
        session_start_id = target_msg_id
        is_catch_up_complete = False  # Reset to catch-up mode
        logging.info(f"🔄 Restarted forwarding from message ID: {target_msg_id}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to restart from message ID {target_msg_id}: {e}")
        return False

async def send_progress_bar():
    """Send progress statistics to progress channel"""
    elapsed_minutes = max((time.time() - start_time) / 60, 1)
    speed = round(stats['forwarded'] / elapsed_minutes, 2)
    current_last_id = await get_last_message_id()
    
    mode = "🔴 Live Mode" if is_catch_up_complete else "🟡 Catch-up Mode"
    session_processed = current_last_id - session_start_id if session_start_id else 0

    text = f"""
╔════❰ ғᴏʀᴡᴀʀᴅ sᴛᴀᴛᴜs ❱═❍⊱❁
║┣⪼ ᴍᴏᴅᴇ: {mode}
║┣⪼ sᴇssɪᴏɴ sᴛᴀʀᴛᴇᴅ ғʀᴏᴍ: {session_start_id}
║┣⪼ ᴄᴜʀʀᴇɴᴛ ʟᴀsᴛ ɪᴅ: {current_last_id}
║┣⪼ ᴛᴏᴛᴀʟ ᴄʜᴇᴄᴋᴇᴅ: {stats['total_messages']}
║┣⪼ ғᴏʀᴡᴀʀᴅᴇᴅ: {stats['forwarded']}
║┣⪼ sᴋɪᴘᴘᴇᴅ: {stats['skipped']}
║┣⪼ ᴇʀʀᴏʀs: {stats['errors']}
║┣⪼ sᴘᴇᴇᴅ: {speed} ᴍsɢs/ᴍɪɴ
║┣⪼ ᴘʀᴏɢʀᴇss: {session_processed} ᴍsɢs ᴘʀᴏᴄᴇssᴇᴅ
║┣⪼ ᴜᴘᴛɪᴍᴇ: {round(elapsed_minutes, 1)} ᴍɪɴᴜᴛᴇs
╚════❰ ᴘʀᴏɢʀᴇssɪɴɢ ❱══❍⊱❁
"""
    try:
        await client.send_message(PROGRESS_CHANNEL, text)
        logging.info("📊 Progress bar sent.")
    except Exception as e:
        logging.error(f"⚠️ Could not send progress bar: {e}")

# Command handlers - keeping original logic
@client.on(events.NewMessage(chats=PROGRESS_CHANNEL, pattern=r'^!(stats|ping|db|restart)(?:\s+(\d+))?$'))
async def handle_commands(event):
    """Handle bot commands from progress channel"""
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
                f"• ID {msg.get('id', 'N/A')}: {str(msg.get('song', 'Unknown'))[:30]}... - {msg.get('status', 'unknown')}"
                for msg in report.get('recent_messages', [])[:5]
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
║┣⪼ ʟɪᴠᴇ sᴛᴀᴛs: ᴛ:{report.get('stats', {}).get('total_messages', 0)} ғ:{report.get('stats', {}).get('forwarded', 0)} s:{report.get('stats', {}).get('skipped', 0)}
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

# Live message handler for new messages (only active after catch-up is complete)
@client.on(events.NewMessage(chats=SOURCE_CHANNEL))
async def handle_new_message(event):
    """Handle new incoming messages in live mode"""
    if not is_catch_up_complete:
        return  # Skip if still in catch-up mode
    
    if is_paused:
        return  # Skip if paused
    
    msg = event.message
    logging.info(f"🔴 Live message received: ID {msg.id}")
    
    # Check if message is newer than our last processed ID
    last_id = await get_last_message_id()
    if msg.id <= last_id:
        return  # Already processed or older
    
    try:
        await process_message(msg)
        await save_last_message_id(msg.id)
        logging.info(f"✅ Live forwarded message {msg.id}")
    except Exception as e:
        logging.error(f"❌ Error in live forwarding message {msg.id}: {e}")

async def process_message(msg):
    """Process and forward a single message - preserves original caption logic"""
    stats["total_messages"] += 1
    
    # Extract Spotify ID from caption
    track_info = extract_spotify_from_msg(msg)
    track_id = track_info.get("track_id") or "N/A"

    # Get song & artist metadata
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

    # Check if source is Spotifyapk56 or similar music channel - preserving original logic
    source_str = str(SOURCE_CHANNEL).lower()
    is_spotify_channel = ('spotifyapk' in source_str or 
                        'spotify' in source_str or 
                        SOURCE_CHANNEL == 'Spotifyapk56' or 
                        SOURCE_CHANNEL == '@Spotifyapk56')
    
    if is_spotify_channel:
        # Build enhanced caption for Spotify channel with track ID
        caption_parts = [f"🎵 {song_name}", f"👤 {artist_name}"]
        
        if track_id != "N/A":
            caption_parts.append(f"🆔 {track_id}")
        caption_text = "\n".join(caption_parts)
    else:
        # For other channels, use original caption as-is
        caption_text = msg.message if msg.message else None

    # Forward message with new caption
    if hasattr(msg, 'media') and msg.media:
        await client.send_file(TARGET_CHANNEL, msg.media, caption=caption_text or "")
    else:
        await client.forward_messages(TARGET_CHANNEL, msg)

    # Save message record to database
    await save_message_record(msg.id, track_id, song_name, artist_name, "forwarded")
    
    stats["forwarded"] += 1

async def sequential_catch_up():
    """Sequential message processing from last_id + 1, incrementing by 1"""
    global is_catch_up_complete
    
    last_id = await get_last_message_id()
    current_id = last_id + 1
    
    logging.info(f"🟡 Starting sequential catch-up from message ID: {current_id}")
    
    consecutive_missing = 0
    max_consecutive_missing = 100  # Stop after 100 consecutive missing messages
    
    while consecutive_missing < max_consecutive_missing:
        if is_paused:
            await asyncio.sleep(1)
            continue
            
        try:
            # Fetch specific message by ID
            msg = await client.get_messages(SOURCE_CHANNEL, ids=current_id)
            
            if msg is None:
                # Message doesn't exist or is deleted
                logging.info(f"⚠️ Message ID {current_id} not found, skipping...")
                stats["skipped"] += 1
                consecutive_missing += 1
            else:
                # Reset consecutive missing counter
                consecutive_missing = 0
                
                try:
                    await process_message(msg)
                    logging.info(f"✅ Sequential forwarded message {current_id}")
                except Exception as e:
                    stats["errors"] += 1
                    await save_message_record(current_id, "N/A", "Error", "Error", "error")
                    logging.error(f"❌ Error processing message {current_id}: {e}")
                    
                # Apply forwarding speed delay
                await asyncio.sleep(forwarding_speed)
            
            # Update last processed ID and move to next
            await save_last_message_id(current_id)
            current_id += 1
            
        except Exception as e:
            logging.error(f"❌ Error fetching message {current_id}: {e}")
            stats["errors"] += 1
            consecutive_missing += 1
            current_id += 1
            await asyncio.sleep(2)  # Brief pause on errors
    
    # Switch to live mode after catch-up is complete
    is_catch_up_complete = True
    logging.info(f"🔴 Catch-up complete! Switching to live mode. Last processed ID: {current_id - 1}")
    
    try:
        await client.send_message(PROGRESS_CHANNEL, 
            f"✅ **Catch-up Complete**\n"
            f"📍 Last processed: ID {current_id - 1}\n"
            f"🔴 Switched to live mode\n"
            f"📊 Session stats: {stats['forwarded']} forwarded, {stats['skipped']} skipped")
    except Exception as e:
        logging.error(f"Could not send catch-up complete notification: {e}")

async def main():
    """Main function - entry point"""
    global session_start_id
    
    logging.info("🚀 Starting Telegram Bot main function...")
    
    # Set shared loop for Flask API
    loop = asyncio.get_event_loop()
    app.set_shared_loop(loop)
    
    # Start Telethon client
    await client.start()
    me = await client.get_me()
    logging.info(f"✅ Logged in as {me.first_name} ({me.id})")

    # Validate channels
    try:
        src_chat = await client.get_entity(SOURCE_CHANNEL)
        if hasattr(src_chat, 'title'):
            logging.info(f"📡 Source channel resolved: {src_chat.title} ({src_chat.id})")
        else:
            logging.info(f"📡 Source channel resolved: {SOURCE_CHANNEL} ({src_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve source channel: {e}")
        return

    try:
        tgt_chat = await client.get_entity(TARGET_CHANNEL)
        if hasattr(tgt_chat, 'title'):
            logging.info(f"📡 Target channel resolved: {tgt_chat.title} ({tgt_chat.id})")
        else:
            logging.info(f"📡 Target channel resolved: {TARGET_CHANNEL} ({tgt_chat.id})")
    except Exception as e:
        logging.error(f"❌ Could not resolve target channel: {e}")
        return

    # Initialize session start point
    current_last_id = await get_last_message_id()
    session_start_id = current_last_id + 1
    
    if current_last_id < 18246934:
        await restart_from_message_id(18246934)
        session_start_id = 18246934
        logging.info("🔄 Initialized starting point to message ID: 18246934")

    # Send startup notification
    try:
        await client.send_message(PROGRESS_CHANNEL, 
            f"🚀 **Forwarder Bot Started**\n"
            f"📍 Starting sequential catch-up from: ID {session_start_id}\n"
            f"📡 Source: https://t.me/{SOURCE_CHANNEL}\n"
            f"🎯 Target: {TARGET_CHANNEL}\n"
            f"🔧 Commands: !stats !db !ping !restart <id>\n"
            f"⚡ Speed: {forwarding_speed}s delay")
    except Exception as e:
        logging.error(f"Could not send startup notification: {e}")

    # Start sequential catch-up process
    asyncio.create_task(sequential_catch_up())

    # Keep the main loop running
    while True:
        await asyncio.sleep(60)
        
        # Send periodic status updates every 10 minutes
        if int(time.time()) % 600 == 0:
            try:
                await send_progress_bar()
            except Exception as e:
                logging.error(f"Error sending periodic update: {e}")

if __name__ == "__main__":
    print("🚀 Starting main.py execution...")
    print("⏰ Waiting for Flask server to initialize...")
    import time
    time.sleep(3)  # Give Flask server time to start
    print("🤖 Now starting Telegram bot main function...")
    asyncio.run(main())
