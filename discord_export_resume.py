#!/usr/bin/env python3
"""
Resume Discord Export Tool

This script helps resume interrupted exports and retry failed channels.
"""

import json
import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_progress(progress_file: Path):
    """Load export progress."""
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            return json.load(f)
    else:
        logger.error(f"Progress file not found: {progress_file}")
        return None

def retry_failed_exports(token: str, output_dir: str = "discord_exports"):
    """Retry all failed channel exports."""
    progress_file = Path(output_dir) / "export_progress.json"
    progress = load_progress(progress_file)
    
    if not progress or not progress.get('failed_channels'):
        logger.info("No failed channels to retry.")
        return
    
    failed_channels = progress['failed_channels']
    logger.info(f"Found {len(failed_channels)} failed channels to retry")
    
    # Clear failed list for fresh retry
    progress['failed_channels'] = []
    
    # Retry each failed channel
    for i, failed in enumerate(failed_channels, 1):
        channel_id = failed['channel_id']
        guild_name = failed['guild_name']
        channel_name = failed['channel_name']
        
        logger.info(f"🔄 Retrying {i}/{len(failed_channels)}: {guild_name}#{channel_name}")
        
        # Skip if already completed
        if channel_id in progress['completed_channels']:
            logger.info(f"✅ Already completed: {guild_name}#{channel_name}")
            continue
        
        # Build export command
        safe_guild = "".join(c for c in guild_name if c.isalnum() or c in "._- ").strip()
        safe_channel = "".join(c for c in channel_name if c.isalnum() or c in "._- ").strip()
        
        guild_dir = Path(output_dir) / safe_guild
        guild_dir.mkdir(exist_ok=True)
        
        cmd = [
            './DiscordChatExporter.Cli',
            'export',
            '-t', token,
            '-c', channel_id,
            '-f', 'HtmlDark',
            '-o', str(guild_dir / f"{safe_channel}.html"),
            '--media', '--reuse-media'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully exported {guild_name}#{channel_name}")
                progress['completed_channels'].append(channel_id)
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"❌ Still failed: {guild_name}#{channel_name}: {error_msg[:100]}")
                progress['failed_channels'].append(failed)
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout: {guild_name}#{channel_name}")
            progress['failed_channels'].append(failed)
        except Exception as e:
            logger.error(f"❌ Error: {guild_name}#{channel_name}: {e}")
            progress['failed_channels'].append(failed)
    
    # Save updated progress
    progress['last_updated'] = datetime.now().isoformat()
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=2)
    
    logger.info(f"🎉 Retry completed. {len(progress['failed_channels'])} channels still failed.")

def show_export_status(output_dir: str = "discord_exports"):
    """Show current export status."""
    progress_file = Path(output_dir) / "export_progress.json"
    progress = load_progress(progress_file)
    
    if not progress:
        logger.info("No export progress found.")
        return
    
    completed = len(progress.get('completed_channels', []))
    failed = len(progress.get('failed_channels', []))
    total = completed + failed
    
    print(f"\n📊 Export Status:")
    print(f"   Total channels processed: {total}")
    print(f"   ✅ Completed: {completed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📅 Last updated: {progress.get('last_updated', 'Unknown')}")
    
    if failed > 0:
        print(f"\n❌ Failed channels:")
        for failed_ch in progress['failed_channels'][:10]:  # Show first 10
            print(f"   - {failed_ch['guild_name']}#{failed_ch['channel_name']}")
        if failed > 10:
            print(f"   ... and {failed - 10} more")
    
    print(f"\n📁 Exports saved to: {Path(output_dir).absolute()}")

def export_specific_channels(token: str, channel_ids: list, output_dir: str = "discord_exports"):
    """Export specific channels by ID."""
    logger.info(f"Exporting {len(channel_ids)} specific channels...")
    
    for i, channel_id in enumerate(channel_ids, 1):
        logger.info(f"📤 Exporting {i}/{len(channel_ids)}: Channel ID {channel_id}")
        
        # Use generic naming since we don't know guild/channel names
        output_file = Path(output_dir) / f"channel_{channel_id}.html"
        
        cmd = [
            './DiscordChatExporter.Cli',
            'export',
            '-t', token,
            '-c', channel_id,
            '-f', 'HtmlDark',
            '-o', str(output_file),
            '--media', '--reuse-media'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully exported channel {channel_id}")
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"❌ Failed to export channel {channel_id}: {error_msg[:100]}")
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout exporting channel {channel_id}")
        except Exception as e:
            logger.error(f"❌ Error exporting channel {channel_id}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Resume and manage Discord exports')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show export status')
    status_parser.add_argument('--output', '-o', default='discord_exports', help='Export directory')
    
    # Retry command
    retry_parser = subparsers.add_parser('retry', help='Retry failed exports')
    retry_parser.add_argument('token', help='Your Discord token')
    retry_parser.add_argument('--output', '-o', default='discord_exports', help='Export directory')
    
    # Export specific channels
    export_parser = subparsers.add_parser('export', help='Export specific channels')
    export_parser.add_argument('token', help='Your Discord token')
    export_parser.add_argument('channels', nargs='+', help='Channel IDs to export')
    export_parser.add_argument('--output', '-o', default='discord_exports', help='Export directory')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'status':
        show_export_status(args.output)
    
    elif args.command == 'retry':
        retry_failed_exports(args.token, args.output)
    
    elif args.command == 'export':
        export_specific_channels(args.token, args.channels, args.output)

if __name__ == "__main__":
    main()