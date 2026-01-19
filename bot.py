import os
import json
import asyncio
from copybot import CopyBot
from pyrogram import Client
from dotenv import load_dotenv

load_dotenv()

topic_filters = {}

async def main():
    try:
        API_ID = os.getenv('API_ID')
        API_HASH = os.getenv('API_HASH')
        
        if not API_ID or not API_HASH:
            raise Exception("Required API credentials (API_ID, API_HASH) not found in .env")
        
        DATABASE_URL = os.getenv('DATABASE_URL')
        if not DATABASE_URL:
            print("⚠️  DATABASE_URL not found in .env - database features disabled")
            DATABASE_URL = None
        
        r2_config = {
            'account_id': os.getenv('R2_ACCOUNT_ID'),
            'access_key_id': os.getenv('R2_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('R2_SECRET_ACCESS_KEY'),
            'bucket_name': os.getenv('R2_BUCKET_NAME'),
            'public_url': os.getenv('R2_PUBLIC_URL')
        }
        
        if DATABASE_URL:
            print(f"📊 Database URL configured for Neon DB")
        
        r2_enabled = all(r2_config.values())
        
        if not r2_enabled:
            print("⚠️  R2 storage not configured - media will not be uploaded to R2")
            r2_config = None
        
        source_channels_json = os.getenv('SOURCE_CHANNELS')
        if not source_channels_json:
            raise Exception("SOURCE_CHANNELS not found in .env")
        
        source_channels = json.loads(source_channels_json)
        if not isinstance(source_channels, list) or not source_channels:
            raise Exception("SOURCE_CHANNELS must be a non-empty JSON array of channel usernames/IDs")
        
        target_channel = os.getenv('TARGET_CHANNEL')
        if not target_channel:
            raise Exception("TARGET_CHANNEL not found in .env")
        
        check_limit = int(os.getenv('CHECK_LIMIT', 10))
        
        copy_bot = CopyBot(
            source_channels, 
            target_channel, 
            API_ID, 
            API_HASH, 
            check_limit,
            topic_filters=topic_filters,
            database_url=DATABASE_URL,
            r2_config=r2_config
        )
        
        await copy_bot.auth()
        
        print("=" * 60)
        print("\n✅ Bot started successfully!")
        print(f"📋 Monitoring channels: {source_channels}")
        print(f"🎯 Copying to: {target_channel}")
        print(f"🔍 Topic filters: {topic_filters if topic_filters else 'None (copying all topics)'}")
        print(f"⏱️  Check interval: 2 seconds")
        print(f"📋 Check limit: {check_limit} messages per channel")
        print(f"💾 Database: {'Neon DB (PostgreSQL)' if DATABASE_URL else 'Disabled'}")
        if r2_enabled:
            print(f"☁️  Storage: Cloudflare R2 (Bucket: {r2_config['bucket_name']})")
        else:
            print(f"☁️  Storage: R2 Disabled")
        print("\n🤖 Bot is now running... Press Ctrl+C to stop\n")
        
        try:
            await copy_bot.start()
        finally:
            await copy_bot.cleanup()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Bot stopped by user")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())