#!/usr/bin/env python3
"""
Discord Content Dumper Bot

This bot can dump Discord channel content including messages, attachments, and images.
Supports multiple export formats: JSON, CSV, and HTML.
"""

import discord
from discord.ext import commands
import json
import csv
import asyncio
import aiohttp
import os
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional, List, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DiscordDumper(commands.Bot):
    def __init__(self, output_dir: str = "discord_dumps"):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        super().__init__(command_prefix='!dump', intents=intents)
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    async def on_ready(self):
        logger.info(f'{self.user} has connected to Discord!')
        logger.info(f'Bot is in {len(self.guilds)} guilds')
        
    async def download_attachment(self, attachment: discord.Attachment, download_dir: Path) -> str:
        """Download an attachment and return the local file path."""
        try:
            download_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean filename
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
    
    async def dump_channel_messages(self, channel: discord.TextChannel, 
                                  limit: Optional[int] = None,
                                  download_media: bool = True) -> List[Dict[str, Any]]:
        """Dump all messages from a channel."""
        messages = []
        media_dir = self.output_dir / f"media_{channel.guild.name}_{channel.name}"
        
        logger.info(f"Dumping messages from #{channel.name} in {channel.guild.name}")
        
        message_count = 0
        async for message in channel.history(limit=limit, oldest_first=True):
            message_count += 1
            if message_count % 100 == 0:
                logger.info(f"Processed {message_count} messages...")
            
            # Download attachments
            attachments_info = []
            for attachment in message.attachments:
                if download_media:
                    local_path = await self.download_attachment(attachment, media_dir)
                    attachments_info.append({
                        'filename': attachment.filename,
                        'url': attachment.url,
                        'local_path': local_path,
                        'size': attachment.size,
                        'content_type': attachment.content_type
                    })
                else:
                    attachments_info.append({
                        'filename': attachment.filename,
                        'url': attachment.url,
                        'size': attachment.size,
                        'content_type': attachment.content_type
                    })
            
            # Handle embeds (for images/videos in messages)
            embeds_info = []
            for embed in message.embeds:
                embed_data = {
                    'title': embed.title,
                    'description': embed.description,
                    'url': embed.url,
                    'type': embed.type
                }
                
                if embed.image:
                    embed_data['image_url'] = embed.image.url
                    if download_media:
                        # Download embed images
                        try:
                            async with aiohttp.ClientSession() as session:
                                async with session.get(embed.image.url) as resp:
                                    if resp.status == 200:
                                        img_filename = f"embed_{message.id}_{embed.image.url.split('/')[-1]}"
                                        img_path = media_dir / img_filename
                                        media_dir.mkdir(parents=True, exist_ok=True)
                                        with open(img_path, 'wb') as f:
                                            f.write(await resp.read())
                                        embed_data['image_local_path'] = str(img_path)
                        except Exception as e:
                            logger.error(f"Error downloading embed image: {e}")
                
                embeds_info.append(embed_data)
            
            # Compile message data
            message_data = {
                'id': message.id,
                'author': {
                    'name': message.author.name,
                    'display_name': message.author.display_name,
                    'id': message.author.id,
                    'bot': message.author.bot
                },
                'content': message.content,
                'timestamp': message.created_at.isoformat(),
                'edited_timestamp': message.edited_at.isoformat() if message.edited_at else None,
                'channel': {
                    'name': channel.name,
                    'id': channel.id
                },
                'guild': {
                    'name': channel.guild.name,
                    'id': channel.guild.id
                },
                'attachments': attachments_info,
                'embeds': embeds_info,
                'reactions': [
                    {
                        'emoji': str(reaction.emoji),
                        'count': reaction.count
                    } for reaction in message.reactions
                ],
                'reply_to': message.reference.message_id if message.reference else None
            }
            
            messages.append(message_data)
        
        logger.info(f"Dumped {len(messages)} messages from #{channel.name}")
        return messages
    
    def save_as_json(self, messages: List[Dict[str, Any]], filename: str):
        """Save messages as JSON file."""
        filepath = self.output_dir / f"{filename}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved JSON to {filepath}")
    
    def save_as_csv(self, messages: List[Dict[str, Any]], filename: str):
        """Save messages as CSV file."""
        filepath = self.output_dir / f"{filename}.csv"
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            if not messages:
                return
            
            writer = csv.writer(f)
            writer.writerow([
                'Message ID', 'Author Name', 'Author ID', 'Content', 'Timestamp',
                'Channel Name', 'Guild Name', 'Attachments', 'Has Embeds', 'Reactions'
            ])
            
            for msg in messages:
                writer.writerow([
                    msg['id'],
                    msg['author']['name'],
                    msg['author']['id'],
                    msg['content'],
                    msg['timestamp'],
                    msg['channel']['name'],
                    msg['guild']['name'],
                    len(msg['attachments']),
                    len(msg['embeds']) > 0,
                    len(msg['reactions'])
                ])
        logger.info(f"Saved CSV to {filepath}")
    
    def save_as_html(self, messages: List[Dict[str, Any]], filename: str):
        """Save messages as HTML file."""
        filepath = self.output_dir / f"{filename}.html"
        
        html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Discord Export</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .message { border-bottom: 1px solid #ccc; padding: 10px; margin: 10px 0; }
        .author { font-weight: bold; color: #7289da; }
        .timestamp { color: #666; font-size: 0.9em; }
        .content { margin: 5px 0; }
        .attachment { background: #f0f0f0; padding: 5px; margin: 5px 0; border-radius: 3px; }
        .embed { background: #f9f9f9; padding: 10px; margin: 5px 0; border-left: 4px solid #7289da; }
        img { max-width: 400px; max-height: 300px; }
    </style>
</head>
<body>
"""
        
        for msg in messages:
            html_content += f"""
<div class="message">
    <div class="author">{msg['author']['name']}</div>
    <div class="timestamp">{msg['timestamp']}</div>
    <div class="content">{msg['content']}</div>
"""
            
            for attachment in msg['attachments']:
                if attachment.get('local_path') and any(ext in attachment['filename'].lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                    html_content += f'<div class="attachment"><img src="{attachment["local_path"]}" alt="{attachment["filename"]}"><br>{attachment["filename"]}</div>'
                else:
                    html_content += f'<div class="attachment">📎 {attachment["filename"]} ({attachment.get("size", 0)} bytes)</div>'
            
            for embed in msg['embeds']:
                html_content += f'<div class="embed">'
                if embed.get('title'):
                    html_content += f'<strong>{embed["title"]}</strong><br>'
                if embed.get('description'):
                    html_content += f'{embed["description"]}<br>'
                if embed.get('image_local_path'):
                    html_content += f'<img src="{embed["image_local_path"]}" alt="Embed image"><br>'
                html_content += '</div>'
            
            html_content += "</div>"
        
        html_content += "</body></html>"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"Saved HTML to {filepath}")

# Bot commands
bot = DiscordDumper()

@bot.command(name='channel')
async def dump_channel(ctx, channel_id: int = None, limit: int = None, format: str = 'json', download_media: bool = True):
    """
    Dump messages from a channel.
    Usage: !dumpchannel [channel_id] [limit] [format] [download_media]
    
    channel_id: ID of channel to dump (default: current channel)
    limit: Number of messages to dump (default: all)
    format: Output format - json, csv, html, or all (default: json)
    download_media: Whether to download images/attachments (default: True)
    """
    try:
        if channel_id:
            channel = bot.get_channel(channel_id)
            if not channel:
                await ctx.send(f"Channel with ID {channel_id} not found.")
                return
        else:
            channel = ctx.channel
        
        await ctx.send(f"Starting dump of #{channel.name}...")
        
        messages = await bot.dump_channel_messages(channel, limit, download_media)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{channel.guild.name}_{channel.name}_{timestamp}"
        
        if format.lower() in ['json', 'all']:
            bot.save_as_json(messages, filename)
        if format.lower() in ['csv', 'all']:
            bot.save_as_csv(messages, filename)
        if format.lower() in ['html', 'all']:
            bot.save_as_html(messages, filename)
        
        await ctx.send(f"Dump completed! {len(messages)} messages exported.")
        
    except Exception as e:
        logger.error(f"Error during dump: {e}")
        await ctx.send(f"Error occurred: {e}")

@bot.command(name='guild')
async def dump_guild(ctx, guild_id: int = None, limit: int = None):
    """
    Dump all channels in a guild.
    Usage: !dumpguild [guild_id] [limit]
    """
    try:
        if guild_id:
            guild = bot.get_guild(guild_id)
            if not guild:
                await ctx.send(f"Guild with ID {guild_id} not found.")
                return
        else:
            guild = ctx.guild
        
        await ctx.send(f"Starting dump of entire guild: {guild.name}")
        
        for channel in guild.text_channels:
            try:
                await ctx.send(f"Dumping #{channel.name}...")
                messages = await bot.dump_channel_messages(channel, limit)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{guild.name}_{channel.name}_{timestamp}"
                bot.save_as_json(messages, filename)
                
            except discord.Forbidden:
                await ctx.send(f"No permission to read #{channel.name}, skipping...")
            except Exception as e:
                await ctx.send(f"Error dumping #{channel.name}: {e}")
        
        await ctx.send("Guild dump completed!")
        
    except Exception as e:
        logger.error(f"Error during guild dump: {e}")
        await ctx.send(f"Error occurred: {e}")

@bot.command(name='help')
async def help_command(ctx):
    """Show help information."""
    help_text = """
**Discord Dumper Bot Commands:**

`!dumpchannel [channel_id] [limit] [format] [download_media]`
- Dump messages from a channel
- channel_id: Channel ID (default: current channel)
- limit: Message limit (default: all messages)
- format: json/csv/html/all (default: json)
- download_media: true/false (default: true)

`!dumpguild [guild_id] [limit]`
- Dump all channels in a guild
- guild_id: Guild ID (default: current guild)
- limit: Message limit per channel

`!dumphelp`
- Show this help message

**Examples:**
- `!dumpchannel` - Dump current channel as JSON
- `!dumpchannel 123456789 1000 html true` - Dump 1000 messages as HTML with media
- `!dumpguild` - Dump entire current server
"""
    await ctx.send(help_text)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python discord_dumper_bot.py <BOT_TOKEN>")
        print("Get your bot token from https://discord.com/developers/applications")
        sys.exit(1)
    
    token = sys.argv[1]
    bot.run(token)