#!/usr/bin/env python3
"""
Bulk Discord Channel Exporter using DiscordChatExporter

This script automatically discovers and exports all channels your Discord account has access to.
Uses DiscordChatExporter CLI tool for safe, reliable exports.
"""

import subprocess
import json
import asyncio
import aiohttp
import argparse
import os
import sys
from pathlib import Path
from datetime import datetime
import time
import logging
from typing import List, Dict, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discord_export.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DiscordBulkExporter:
    def __init__(self, token: str, output_dir: str = "discord_exports"):
        self.token = token
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Export settings
        self.export_format = "HtmlDark"  # HtmlDark, Json, PlainText, Csv
        self.download_media = True
        self.date_limit = None  # Export all messages by default
        
        # Rate limiting
        self.delay_between_exports = 2  # seconds
        self.max_retries = 3
        
        # Progress tracking
        self.progress_file = self.output_dir / "export_progress.json"
        self.load_progress()
    
    def load_progress(self):
        """Load previous export progress."""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                self.progress = json.load(f)
        else:
            self.progress = {
                'completed_channels': [],
                'failed_channels': [],
                'last_updated': None
            }
    
    def save_progress(self):
        """Save current export progress."""
        self.progress['last_updated'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    async def get_user_guilds(self) -> List[Dict[str, Any]]:
        """Get all guilds (servers) the user has access to."""
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get('https://discord.com/api/v10/users/@me/guilds', headers=headers) as resp:
                if resp.status == 200:
                    guilds = await resp.json()
                    logger.info(f"Found {len(guilds)} accessible guilds")
                    return guilds
                else:
                    logger.error(f"Failed to fetch guilds: {resp.status}")
                    return []
    
    async def get_guild_channels(self, guild_id: str) -> List[Dict[str, Any]]:
        """Get all channels in a guild."""
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(f'https://discord.com/api/v10/guilds/{guild_id}/channels', headers=headers) as resp:
                if resp.status == 200:
                    channels = await resp.json()
                    # Filter to text channels only
                    text_channels = [ch for ch in channels if ch.get('type') in [0, 5, 10, 11, 12]]  # Text channel types
                    return text_channels
                else:
                    logger.warning(f"Failed to fetch channels for guild {guild_id}: {resp.status}")
                    return []
    
    async def get_dm_channels(self) -> List[Dict[str, Any]]:
        """Get all DM channels."""
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get('https://discord.com/api/v10/users/@me/channels', headers=headers) as resp:
                if resp.status == 200:
                    dm_channels = await resp.json()
                    logger.info(f"Found {len(dm_channels)} DM channels")
                    return dm_channels
                else:
                    logger.error(f"Failed to fetch DM channels: {resp.status}")
                    return []
    
    def export_channel(self, channel_id: str, guild_name: str = "DM", channel_name: str = "unknown") -> bool:
        """Export a single channel using DiscordChatExporter."""
        try:
            # Skip if already exported
            if channel_id in self.progress['completed_channels']:
                logger.info(f"Skipping already exported channel: {guild_name}#{channel_name}")
                return True
            
            # Create safe filenames
            safe_guild = "".join(c for c in guild_name if c.isalnum() or c in "._- ").strip()
            safe_channel = "".join(c for c in channel_name if c.isalnum() or c in "._- ").strip()
            
            # Create output directory for this guild
            guild_dir = self.output_dir / safe_guild
            guild_dir.mkdir(exist_ok=True)
            
            # Build DiscordChatExporter command
            cmd = [
                './DiscordChatExporter.Cli',
                'export',
                '-t', self.token,
                '-c', channel_id,
                '-f', self.export_format,
                '-o', str(guild_dir / f"{safe_channel}.html")
            ]
            
            # Add media download option
            if self.download_media:
                cmd.extend(['--media', '--reuse-media'])
            
            # Add date limit if specified
            if self.date_limit:
                cmd.extend(['--after', self.date_limit])
            
            logger.info(f"Exporting {guild_name}#{channel_name} (ID: {channel_id})")
            
            # Run the export command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully exported {guild_name}#{channel_name}")
                self.progress['completed_channels'].append(channel_id)
                self.save_progress()
                return True
            else:
                error_msg = result.stderr or result.stdout
                if "Unauthorized" in error_msg or "403" in error_msg:
                    logger.warning(f"⚠️  No access to {guild_name}#{channel_name}")
                elif "Not Found" in error_msg or "404" in error_msg:
                    logger.warning(f"⚠️  Channel not found: {guild_name}#{channel_name}")
                else:
                    logger.error(f"❌ Failed to export {guild_name}#{channel_name}: {error_msg}")
                    self.progress['failed_channels'].append({
                        'channel_id': channel_id,
                        'guild_name': guild_name,
                        'channel_name': channel_name,
                        'error': error_msg
                    })
                self.save_progress()
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout exporting {guild_name}#{channel_name}")
            return False
        except Exception as e:
            logger.error(f"❌ Error exporting {guild_name}#{channel_name}: {e}")
            return False
    
    async def discover_and_export_all(self):
        """Discover all accessible channels and export them."""
        logger.info("🔍 Discovering all accessible Discord channels...")
        
        all_channels = []
        
        # Get guild channels
        guilds = await self.get_user_guilds()
        for guild in guilds:
            guild_id = guild['id']
            guild_name = guild['name']
            
            logger.info(f"📋 Fetching channels for guild: {guild_name}")
            channels = await self.get_guild_channels(guild_id)
            
            for channel in channels:
                all_channels.append({
                    'id': channel['id'],
                    'name': channel.get('name', 'unknown'),
                    'guild_name': guild_name,
                    'type': 'guild'
                })
        
        # Get DM channels
        dm_channels = await self.get_dm_channels()
        for dm in dm_channels:
            recipient_name = "Unknown"
            if dm.get('recipients'):
                recipient_name = dm['recipients'][0].get('username', 'Unknown')
            elif dm.get('name'):
                recipient_name = dm['name']
            
            all_channels.append({
                'id': dm['id'],
                'name': f"DM-{recipient_name}",
                'guild_name': "DirectMessages",
                'type': 'dm'
            })
        
        logger.info(f"📊 Found {len(all_channels)} total channels to export")
        
        # Export all channels
        successful_exports = 0
        failed_exports = 0
        
        for i, channel in enumerate(all_channels, 1):
            logger.info(f"📤 Exporting {i}/{len(all_channels)}: {channel['guild_name']}#{channel['name']}")
            
            success = self.export_channel(
                channel['id'],
                channel['guild_name'],
                channel['name']
            )
            
            if success:
                successful_exports += 1
            else:
                failed_exports += 1
            
            # Rate limiting delay
            if i < len(all_channels):
                time.sleep(self.delay_between_exports)
        
        # Export summary
        logger.info(f"🎉 Export completed!")
        logger.info(f"✅ Successful: {successful_exports}")
        logger.info(f"❌ Failed: {failed_exports}")
        logger.info(f"📁 Exports saved to: {self.output_dir}")
        
        if self.progress['failed_channels']:
            logger.info("❌ Failed channels:")
            for failed in self.progress['failed_channels']:
                logger.info(f"   - {failed['guild_name']}#{failed['channel_name']}: {failed['error'][:100]}")

def main():
    parser = argparse.ArgumentParser(description='Bulk export Discord channels using DiscordChatExporter')
    parser.add_argument('token', help='Your Discord user token')
    parser.add_argument('--output', '-o', default='discord_exports', help='Output directory')
    parser.add_argument('--format', '-f', choices=['HtmlDark', 'Json', 'PlainText', 'Csv'], 
                       default='HtmlDark', help='Export format')
    parser.add_argument('--no-media', action='store_true', help='Skip media downloads')
    parser.add_argument('--after', help='Export messages after this date (YYYY-MM-DD)')
    parser.add_argument('--delay', type=int, default=2, help='Delay between exports (seconds)')
    
    args = parser.parse_args()
    
    # Validate token format
    if not args.token or len(args.token) < 50:
        print("❌ Invalid token format. Please provide a valid Discord token.")
        print("\n🔑 How to get your Discord token:")
        print("1. Open Discord in browser (discord.com)")
        print("2. Press F12 → Console tab") 
        print("3. Paste: (webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m=>m?.exports?.default?.getToken!==void 0).exports.default.getToken()")
        print("4. Copy the returned token")
        sys.exit(1)
    
    # Check if DiscordChatExporter is installed
    try:
        subprocess.run(['DiscordChatExporter.Cli', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ DiscordChatExporter not found!")
        print("📦 Please run: ./setup_discord_exporter.sh")
        sys.exit(1)
    
    # Create exporter instance
    exporter = DiscordBulkExporter(args.token, args.output)
    exporter.export_format = args.format
    exporter.download_media = not args.no_media
    exporter.date_limit = args.after
    exporter.delay_between_exports = args.delay
    
    # Run the export
    try:
        asyncio.run(exporter.discover_and_export_all())
    except KeyboardInterrupt:
        logger.info("❌ Export interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()