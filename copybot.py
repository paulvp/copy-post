import datetime
import asyncio
import os
import re
import asyncpg
import aioboto3
import hashlib
from pathlib import Path
from typing import List, Optional

from pyrogram import Client
from pyrogram.types import Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAnimation, MessageEntity
from pyrogram.errors import MessageIdInvalid, ChatWriteForbidden, MessageEmpty, MessageNotModified, PeerIdInvalid
from dotenv import load_dotenv

load_dotenv()

import os
import json
from typing import Optional, List
import asyncio
import datetime
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import PeerIdInvalid

class CopyBot:
    def __init__(self, source_channels, target_channel, api_id, api_hash, check_limit=10, topic_filters=None, database_url=None, r2_config=None):
        """
        Initialize with source channels and target channel
 
        Args:
            source_channels (list): List of source channel usernames/IDs
            target_channel (str): Target channel username/ID
            api_id (str): Telegram API ID
            api_hash (str): Telegram API Hash
            check_limit (int): Number of recent messages to check per source channel
            topic_filters (dict): Optional dict mapping channel IDs to topic IDs to filter
                                 Example: {-1002651608009: 221}
            database_url (str): PostgreSQL connection string for Neon DB
            r2_config (dict): Cloudflare R2 configuration
        """
        self.source_channels = source_channels
        self.target_channel = target_channel
        self.API_ID = api_id
        self.API_HASH = api_hash
        self.check_limit = check_limit
        self.topic_filters = topic_filters or {}
        self.database_url = database_url
        self.r2_config = r2_config or {}
        self.app = None
        self.db_pool = None
        self.r2_session = None
        
        if self.r2_config.get('access_key_id'):
            self.r2_session = aioboto3.Session(
                aws_access_key_id=self.r2_config['access_key_id'],
                aws_secret_access_key=self.r2_config['secret_access_key']
            )
            print("✓ R2 Storage configured")
        
        self.source_data = {}
        self.last_forwarded_msg_ids = {}
        self.forwarded_media_groups = {}
        
        for channel in source_channels:
            self.source_data[channel] = {
                'last_msg_id': None,
                'last_msg_obj': None,
                'target_channel': target_channel
            }
            self.last_forwarded_msg_ids[channel] = 0
            self.forwarded_media_groups[channel] = set()

    async def connect_db(self):
        """Connect to Neon DB with connection pooling"""
        if not self.database_url:
            return True
        
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
                ssl='require',
                max_inactive_connection_lifetime=300
            )
            print("✓ Connected to Neon DB")
            return True
        except Exception as e:
            print(f"❌ Neon DB connection error: {e}")
            return False

    async def cleanup(self):
        """Clean up connections"""
        if self.db_pool:
            await self.db_pool.close()

    async def is_content_duplicate(self, message_id, channel_id):
        """Check if specific message ID from channel was already processed"""
        if not self.db_pool:
            return False
        
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM content 
                        WHERE message_id = $1 AND channel_id = $2
                        LIMIT 1
                    )
                """, message_id, str(channel_id))
                return result
        except Exception as e:
            print(f"❌ Error checking duplicate: {e}")
            return False

    async def save_content_to_db(self, text, category=None, media_links=None, media_type=None, message_id=None, channel_id=None):
        """Save content to Neon DB"""
        if not self.db_pool:
            return False
        
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO content (text, category, media_links, media_type, message_id, channel_id)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, text, category, media_links or [], media_type, message_id, str(channel_id))
                return True
        except Exception as e:
            print(f"❌ Error saving content: {e}")
            return False

    async def upload_to_r2(self, file_data: bytes, file_name: str, content_type: str = 'application/octet-stream') -> Optional[str]:
        """Upload file to Cloudflare R2"""
        if not self.r2_session:
            return None
        
        try:
            file_hash = hashlib.md5(file_data).hexdigest()[:8]
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_name = f"{timestamp}_{file_hash}_{file_name}"
            
            async with self.r2_session.client(
                's3',
                endpoint_url=f"https://{self.r2_config['account_id']}.r2.cloudflarestorage.com",
                region_name='auto'
            ) as s3:
                await s3.put_object(
                    Bucket=self.r2_config['bucket_name'],
                    Key=unique_name,
                    Body=file_data,
                    ContentType=content_type
                )
            
            public_url = f"{self.r2_config['public_url']}/{unique_name}"
            print(f"  ✓ Uploaded to R2: {unique_name}")
            return public_url
            
        except Exception as e:
            print(f"  ❌ Error uploading to R2: {e}")
            return None

    async def check_content_duplicate(self, message_id, channel_id):
        """Check if message was already processed"""
        return await self.is_content_duplicate(message_id, channel_id)

    async def download_media(self, message: Message) -> Optional[bytes]:
        """Download media from message with timeout"""
        try:
            file_path = await asyncio.wait_for(
                self.app.download_media(message, in_memory=True),
                timeout=60.0
            )
            if file_path:
                return file_path.getvalue()
            return None
        except asyncio.TimeoutError:
            print(f"  ⚠ Media download timeout")
            return None
        except Exception as e:
            print(f"  ⚠ Error downloading media: {e}")
            return None

    async def save_content(self, text, category=None, media_links=None, media_type=None, message_id=None, channel_id=None):
        """Save content to Neon DB with R2 media links"""
        return await self.save_content_to_db(text, category, media_links, media_type, message_id, channel_id)

    def get_message_type(self, message: Message):
        """Get human-readable message type for logging"""
        if message.text and not message.media:
            return "text"
        elif message.photo:
            return "photo"
        elif message.video:
            return "video"
        elif message.animation:
            return "animation"
        elif message.audio:
            return "audio"
        elif message.voice:
            return "voice"
        elif message.video_note:
            return "video_note"
        elif message.sticker:
            return "sticker"
        elif message.document:
            return "document"
        elif message.location:
            return "location"
        elif message.venue:
            return "venue"
        elif message.contact:
            return "contact"
        elif message.poll:
            return "poll"
        elif message.dice:
            return "dice"
        elif message.media_group_id:
            return "media_group"
        else:
            return "unknown"

    def get_message_topic(self, message: Message):
        """Extract topic ID from message - try multiple methods"""
        if hasattr(message, 'topic_id') and message.topic_id:
            return message.topic_id
        
        if hasattr(message, 'reply_to_message_id') and message.reply_to_message_id:
            if hasattr(message, 'reply_to_top_message_id') and message.reply_to_top_message_id:
                return message.reply_to_top_message_id
        
        if hasattr(message, 'reply_to_top_id') and message.reply_to_top_id:
            return message.reply_to_top_id
        
        if hasattr(message, 'reply_to_message') and message.reply_to_message:
            if hasattr(message.reply_to_message, 'id'):
                return message.reply_to_message.id
        
        return None

    def should_filter_message(self, message: Message, channel) -> bool:
        """Check if message should be filtered based on topic"""
        try:
            if isinstance(channel, str):
                channel_int = int(channel)
            else:
                channel_int = channel
        except (ValueError, TypeError):
            return False
        
        if channel_int in self.topic_filters:
            required_topic = self.topic_filters[channel_int]
            message_topic = self.get_message_topic(message)
            
            if message_topic:
                print(f"  [DEBUG] Message {message.id} has topic: {message_topic} (required: {required_topic})")
            
            if message_topic != required_topic:
                if message_topic is not None:
                    print(f"  ⊘ Skipping message {message.id} - wrong topic ({message_topic} != {required_topic})")
                return True 
        
        return False 

    async def upload_media_to_r2(self, message: Message) -> tuple[List[str], str]:
        """Download media from message and upload to R2
        
        Returns:
            Tuple of (list of R2 URLs, media type)
        """
        media_links = []
        media_type = None
        
        if not self.r2_session:
            return media_links, media_type
        
        try:
            if message.photo:
                media_type = 'photo'
                media_data = await self.download_media(message)
                if media_data:
                    file_name = f"photo_{message.photo.file_id}.jpg"
                    url = await self.upload_to_r2(media_data, file_name, 'image/jpeg')
                    if url:
                        media_links.append(url)
            
            elif message.video:
                media_type = 'video'
                media_data = await self.download_media(message)
                if media_data:
                    file_name = f"video_{message.video.file_id}.mp4"
                    url = await self.upload_to_r2(media_data, file_name, 'video/mp4')
                    if url:
                        media_links.append(url)
            
            elif message.animation:
                media_type = 'animation'
                media_data = await self.download_media(message)
                if media_data:
                    file_name = f"animation_{message.animation.file_id}.mp4"
                    url = await self.upload_to_r2(media_data, file_name, 'video/mp4')
                    if url:
                        media_links.append(url)
            
            elif message.document:
                media_type = 'document'
                media_data = await self.download_media(message)
                if media_data:
                    file_name = message.document.file_name or f"document_{message.document.file_id}"
                    mime_type = message.document.mime_type or 'application/octet-stream'
                    url = await self.upload_to_r2(media_data, file_name, mime_type)
                    if url:
                        media_links.append(url)
        
        except Exception as e:
            print(f"  ⚠ Error uploading media to R2: {e}")
        
        return media_links, media_type

    async def forward_media_group(self, messages: List[Message], target_channel: str):
        """Forward entire media group (multiple messages) to target channel"""
        try:
            message_ids = [msg.id for msg in messages]
            
            forwarded = await self.app.forward_messages(
                chat_id=target_channel,
                from_chat_id=messages[0].chat.id,
                message_ids=message_ids
            )
            
            if forwarded:
                print(f"  ✓ Successfully forwarded media group ({len(message_ids)} items)")
                return True
            else:
                print(f"  ✗ Failed to forward media group")
                return False
                
        except Exception as e:
            print(f"  ✗ Error forwarding media group: {e}")
            return False

    async def send_content_with_media(self, text: str, images: List[str], target_channel: str):
        """Send content with associated media to target channel"""
        try:
            await self.app.send_message(
                target_channel,
                text
            )
            print(f"  ✓ Successfully sent content: {text[:50]}{'...' if len(text) > 50 else ''}")
            return True
        except Exception as e:
            print(f"  ✗ Error sending content: {e}")
            return False

    async def resolve_peer_cache(self, chat_id):
        """Load dialogs to populate peer cache for a specific chat"""
        try:
            async for dialog in self.app.get_dialogs():
                if dialog.chat.id == chat_id:
                    return True
        except Exception as e:
            print(f"  ⚠ Error loading dialogs: {e}")
        return False

    async def auth(self):
        """Authenticate with Telegram and connect to database"""
        try:
            if not await self.connect_db():
                print("⚠️  Database connection failed - continuing without database")
            
            self.app = Client(
                "sessions/copy",
                api_id=self.API_ID,
                api_hash=self.API_HASH
            )
            await self.app.start()
            
            print("🔄 Loading dialogs to populate peer cache...")
            dialog_count = 0
            async for dialog in self.app.get_dialogs():
                dialog_count += 1
            print(f"✓ Loaded {dialog_count} chats into cache\n")
            
            print("Verifying source channels:")
            for channel in self.source_channels[:]:
                try:
                    if isinstance(channel, str) and channel.lstrip('-').isdigit():
                        channel_id = int(channel)
                    else:
                        channel_id = channel
                    
                    chat = await self.app.get_chat(channel_id)
                    print(f"  ✓ {chat.title} (ID: {chat.id})")
                    
                    if channel_id in self.topic_filters:
                        topic_id = self.topic_filters[channel_id]
                        print(f"    → Topic filter active: only copying from topic {topic_id}")
                        
                        count = 0
                        topic_count = 0
                        async for message in self.app.get_chat_history(channel_id, limit=20):
                            count += 1
                            msg_topic = self.get_message_topic(message)
                            if msg_topic == topic_id:
                                topic_count += 1
                        print(f"    → Found {topic_count}/{count} recent messages in topic {topic_id}")
                        
                except PeerIdInvalid:
                    print(f"  ✗ Cannot access {channel} - not a member or invalid ID")
                    self.source_channels.remove(channel)
                except Exception as e:
                    print(f"  ✗ Failed to resolve {channel}: {e}")
                    self.source_channels.remove(channel)

            if not self.source_channels:
                raise Exception("No valid source channels found!")

            print("\nVerifying target channel:")
            try:
                chat = await self.app.get_chat(self.target_channel)
                print(f"  ✓ {chat.title} (ID: {chat.id})")
            except Exception as e:
                print(f"  ✗ Cannot access target channel: {e}")
                raise

        except Exception as e:
            print(f"\n❌ Authentication error: {e}")
            raise

    async def get_source_last_posts(self):
        """Get and process last posts from all source channels"""
        for channel in self.source_channels:
            try:
                if isinstance(channel, str) and channel.lstrip('-').isdigit():
                    channel_id = int(channel)
                else:
                    channel_id = channel
                
                messages = []
                async for message in self.app.get_chat_history(channel_id, limit=self.check_limit):
                    if self.should_filter_message(message, channel_id):
                        continue
                    messages.append(message)
                
                if messages:
                    last_forwarded = self.last_forwarded_msg_ids.get(channel, 0)
                    
                    new_messages = [msg for msg in reversed(messages) if msg.id > last_forwarded]
                    
                    media_groups = {}
                    standalone_messages = []
                    
                    for message in new_messages:
                        if message.media_group_id:
                            if message.media_group_id not in media_groups:
                                media_groups[message.media_group_id] = []
                            media_groups[message.media_group_id].append(message)
                        else:
                            standalone_messages.append(message)
                    
                    for media_group_id, group_messages in media_groups.items():
                        if media_group_id in self.forwarded_media_groups.get(channel, set()):
                            continue
                        
                        group_messages.sort(key=lambda m: m.id)
                        
                        self.source_data[channel]['last_msg_id'] = group_messages[-1].id
                        self.source_data[channel]['last_msg_obj'] = group_messages
                        count = await self.match_messages(channel)
                        if count == 1:
                            await asyncio.sleep(1)
                    
                    for message in standalone_messages:
                        self.source_data[channel]['last_msg_id'] = message.id
                        self.source_data[channel]['last_msg_obj'] = message
                        count = await self.match_messages(channel)
                        if count == 1:
                            await asyncio.sleep(1)
            except Exception as e:
                print(f"❌ Error processing channel {channel}: {e}")
                continue

    async def match_messages(self, channel):
        """Check and copy messages for a specific channel"""
        try:
            current_msg_id = self.source_data[channel]['last_msg_id']
            msg_obj = self.source_data[channel]['last_msg_obj']
            target_channel = self.source_data[channel]['target_channel']
            
            is_media_group = isinstance(msg_obj, list)
            messages_to_process = msg_obj if is_media_group else [msg_obj]
            first_msg = messages_to_process[0]
            
            if current_msg_id <= self.last_forwarded_msg_ids.get(channel, 0):
                return 0

            if is_media_group and first_msg.media_group_id in self.forwarded_media_groups.get(channel, set()):
                for msg in messages_to_process:
                    if msg.id > self.last_forwarded_msg_ids.get(channel, 0):
                        self.last_forwarded_msg_ids[channel] = msg.id
                return 0

            message_text = first_msg.text or first_msg.caption or ''
            msg_type = "media_group" if is_media_group else self.get_message_type(first_msg)
            topic_id = self.get_message_topic(first_msg)
            topic_info = f" (topic: {topic_id})" if topic_id else ""
            
            content_preview = ""
            if is_media_group:
                content_preview = f" | Media group ({len(messages_to_process)} items)"
            elif message_text:
                content_preview = f" | Text: {message_text[:40]}{'...' if len(message_text) > 40 else ''}"
            elif first_msg.photo:
                content_preview = " | Photo"
            elif first_msg.video:
                content_preview = " | Video"
            elif first_msg.animation:
                content_preview = " | Animation"
            
            print(f"[{self.get_current_datetime()}] Processing message ID: {current_msg_id} "
                  f"(type: {msg_type}{topic_info}) from: {channel}{content_preview}")

            if isinstance(channel, str) and channel.lstrip('-').isdigit():
                channel_id = int(channel)
            else:
                channel_id = channel

            check_msg_id = messages_to_process[-1].id if is_media_group else current_msg_id
            if await self.check_content_duplicate(check_msg_id, channel_id):
                print(f"  ⊘ Skipping duplicate message (already processed)")
                for msg in messages_to_process:
                    if msg.id > self.last_forwarded_msg_ids.get(channel, 0):
                        self.last_forwarded_msg_ids[channel] = msg.id
                if is_media_group:
                    self.forwarded_media_groups[channel].add(first_msg.media_group_id)
                return 0

            all_media_links = []
            media_type = None
            
            if is_media_group:
                print(f"  → Uploading {len(messages_to_process)} media items to R2...")
            
            for msg in messages_to_process:
                media_links, msg_media_type = await self.upload_media_to_r2(msg)
                all_media_links.extend(media_links)
                if msg_media_type and not media_type:
                    media_type = msg_media_type
            
            category = self.get_category_from_channel(channel)
            
            # Forward messages to target channel
            success = True
            if is_media_group:
                success = await self.forward_media_group(messages_to_process, target_channel)
            else:
                try:
                    forwarded = await self.app.forward_messages(
                        chat_id=target_channel,
                        from_chat_id=first_msg.chat.id,
                        message_ids=first_msg.id
                    )
                    if forwarded:
                        print(f"  ✓ Successfully forwarded message")
                    else:
                        print(f"  ✗ Failed to forward message")
                        success = False
                except Exception as e:
                    print(f"  ✗ Error forwarding message: {e}")
                    success = False
            
            if success:
                save_msg_id = messages_to_process[-1].id if is_media_group else current_msg_id
                save_text = message_text if message_text else f"[{msg_type}]"
                await self.save_content(save_text, category, all_media_links, media_type, save_msg_id, channel_id)
                
                if all_media_links:
                    media_count_text = f"{len(all_media_links)} media item{'s' if len(all_media_links) > 1 else ''}"
                    print(f"  ✓ Forwarded successfully (saved {media_count_text} to R2)")
                else:
                    print(f"  ✓ Forwarded successfully")
            
            for msg in messages_to_process:
                if msg.id > self.last_forwarded_msg_ids.get(channel, 0):
                    self.last_forwarded_msg_ids[channel] = msg.id
            
            if is_media_group:
                self.forwarded_media_groups[channel].add(first_msg.media_group_id)
            
            return 1
        except Exception as e:
            print(f"❌ Error matching messages: {e}")
            if isinstance(msg_obj, list):
                for msg in msg_obj:
                    if msg.id > self.last_forwarded_msg_ids.get(channel, 0):
                        self.last_forwarded_msg_ids[channel] = msg.id
            else:
                self.last_forwarded_msg_ids[channel] = current_msg_id
            return 0

    def get_category_from_channel(self, channel) -> Optional[str]:
        """Get category name based on source channel"""
        return "general"

    def get_current_datetime(self):
        """Get formatted current datetime"""
        now = datetime.datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S")

    async def start(self):
        """Start the copying process"""
        try:
            while True:
                await self.get_source_last_posts()
                await asyncio.sleep(2)  # Check every 2 seconds
        except KeyboardInterrupt:
            print("\n⚠️  Stopping bot...")
            raise
        except Exception as e:
            print(f"❌ Error in start method: {e}")
            raise