#!/usr/bin/env python3
"""
Discord Self-Bot Content Dumper

WARNING: This uses your personal Discord token and violates Discord's Terms of Service.
Use at your own risk - your account could be banned!

This is for educational purposes only.
"""

import discord
import json
import asyncio
import aiohttp
import os
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DiscordSelfBot:
    def __init__(self, token: str, output_dir: str = "discord_selfbot_dumps"):
        self.token = token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create client with user account (not bot)
        self.client = discord.Client(intents=discord.Intents.all())
        
        @self.client.event
        async def on_ready():
            logger.info(f'Logged in as {self.client.user} (ID: {self.client.user.id})')
            logger.info(f'In {len(self.client.guilds)} guilds')
    
    async def get_user_token_instructions(self):
        """Instructions for getting your personal Discord token."""
        instructions = """
        How to get your Discord user token:
        
        1. Open Discord in your web browser (discord.com)
        2. Log in to your account
        3. Press F12 to open Developer Tools
        4. Go to Network tab
        5. Send a message in any channel
        6. Look for a request to "messages" 
        7. In Request Headers, find "authorization" 
        8. Copy the token (it's a long string)
        
        Alternative method:
        1. Press Ctrl+Shift+I (Developer Tools)
        2. Go to Console tab
        3. Type: (webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m=>m?.exports?.default?.getToken!==void 0).exports.default.getToken()
        4. Copy the returned token
        
        IMPORTANT: Never share this token with anyone!
        """
        return instructions
    
    async def download_attachment(self, attachment, download_dir: Path) -> str:
        """Download an attachment and return the local file path."""
        try:
            download_dir.mkdir(parents=True, exist_ok=True)
            
            safe_filename = "".join(c for c in attachment.filename if c.isalnum() or c in "._-")
            file_path = download_dir / f"{attachment.id}_{safe_filename}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(attachment.url) as resp:
                    if resp.status == 200:
                        with open(file_path, 'wb') as f:
                            f.write(await resp.read())
                        logger.info(f"Downloaded: {attachment.filename}")
                        return str(file_path)
                    else:
                        logger.error(f"Failed to download {attachment.filename}: HTTP {resp.status}")
                        return attachment.url
        except Exception as e:
            logger.error(f"Error downloading {attachment.filename}: {e}")
            return attachment.url
    
    async def dump_channel(self, channel_id: int, limit: int = None, download_media: bool = True):
        """Dump messages from a specific channel."""
        try:
            channel = self.client.get_channel(channel_id)
            if not channel:
                logger.error(f"Cannot access channel {channel_id}")
                return []
            
            logger.info(f"Dumping #{channel.name} in {channel.guild.name}")
            
            messages = []
            media_dir = self.output_dir / f"media_{channel.guild.name}_{channel.name}"
            
            async for message in channel.history(limit=limit, oldest_first=True):
                # Download attachments if requested
                attachments_info = []
                for attachment in message.attachments:
                    if download_media:
                        local_path = await self.download_attachment(attachment, media_dir)
                        attachments_info.append({
                            'filename': attachment.filename,
                            'url': attachment.url,
                            'local_path': local_path,
                            'size': attachment.size
                        })
                    else:
                        attachments_info.append({
                            'filename': attachment.filename,
                            'url': attachment.url,
                            'size': attachment.size
                        })
                
                message_data = {
                    'id': message.id,
                    'author': {
                        'name': message.author.name,
                        'display_name': message.author.display_name,
                        'id': message.author.id
                    },
                    'content': message.content,
                    'timestamp': message.created_at.isoformat(),
                    'channel': {
                        'name': channel.name,
                        'id': channel.id
                    },
                    'guild': {
                        'name': channel.guild.name,
                        'id': channel.guild.id
                    },
                    'attachments': attachments_info,
                    'reactions': [
                        {
                            'emoji': str(reaction.emoji),
                            'count': reaction.count
                        } for reaction in message.reactions
                    ]
                }
                
                messages.append(message_data)
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"selfbot_{channel.guild.name}_{channel.name}_{timestamp}.json"
            filepath = self.output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(messages)} messages to {filepath}")
            return messages
            
        except Exception as e:
            logger.error(f"Error dumping channel: {e}")
            return []
    
    async def dump_dm_channel(self, user_id: int, limit: int = None):
        """Dump direct messages with a specific user."""
        try:
            user = self.client.get_user(user_id)
            if not user:
                logger.error(f"Cannot find user {user_id}")
                return []
            
            dm_channel = user.dm_channel
            if not dm_channel:
                dm_channel = await user.create_dm()
            
            logger.info(f"Dumping DMs with {user.name}")
            
            messages = []
            async for message in dm_channel.history(limit=limit, oldest_first=True):
                message_data = {
                    'id': message.id,
                    'author': {
                        'name': message.author.name,
                        'id': message.author.id
                    },
                    'content': message.content,
                    'timestamp': message.created_at.isoformat(),
                    'channel_type': 'dm',
                    'attachments': [
                        {
                            'filename': att.filename,
                            'url': att.url,
                            'size': att.size
                        } for att in message.attachments
                    ]
                }
                messages.append(message_data)
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"selfbot_dm_{user.name}_{timestamp}.json"
            filepath = self.output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(messages)} DM messages to {filepath}")
            return messages
            
        except Exception as e:
            logger.error(f"Error dumping DMs: {e}")
            return []
    
    async def list_accessible_channels(self):
        """List all channels the user can access."""
        channels = []
        for guild in self.client.guilds:
            for channel in guild.text_channels:
                if channel.permissions_for(guild.me).read_messages:
                    channels.append({
                        'guild': guild.name,
                        'channel': channel.name,
                        'id': channel.id,
                        'guild_id': guild.id
                    })
        return channels
    
    async def run(self):
        """Start the self-bot."""
        await self.client.start(self.token)
    
    def close(self):
        """Close the client."""
        asyncio.create_task(self.client.close())

# Example usage
async def main():
    # YOU MUST GET YOUR OWN USER TOKEN - see instructions above
    USER_TOKEN = "YOUR_USER_TOKEN_HERE"
    
    if USER_TOKEN == "YOUR_USER_TOKEN_HERE":
        selfbot = DiscordSelfBot("")
        instructions = await selfbot.get_user_token_instructions()
        print(instructions)
        return
    
    selfbot = DiscordSelfBot(USER_TOKEN)
    
    try:
        # Start the client
        await selfbot.client.start(USER_TOKEN)
        
        # List available channels
        channels = await selfbot.list_accessible_channels()
        print("Available channels:")
        for ch in channels[:10]:  # Show first 10
            print(f"  {ch['guild']} > #{ch['channel']} (ID: {ch['id']})")
        
        # Example: Dump a specific channel
        # await selfbot.dump_channel(CHANNEL_ID, limit=100)
        
        # Example: Dump DMs with a user
        # await selfbot.dump_dm_channel(USER_ID, limit=50)
        
    except discord.LoginFailure:
        print("Invalid token! Please check your user token.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        selfbot.close()

if __name__ == "__main__":
    asyncio.run(main())