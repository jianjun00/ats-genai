#!/usr/bin/env python3
"""
Discord Bot Configuration

Store your bot token and other settings here.
DO NOT commit this file with real tokens!
"""

import os
from pathlib import Path

class DiscordConfig:
    """Configuration for Discord dumper bot."""
    
    # Bot token - Get this from https://discord.com/developers/applications
    BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')
    
    # Output directory for dumps
    OUTPUT_DIR = os.getenv('DISCORD_OUTPUT_DIR', 'discord_dumps')
    
    # Default settings
    DEFAULT_MESSAGE_LIMIT = None  # None = all messages
    DEFAULT_FORMAT = 'json'  # json, csv, html, or all
    DEFAULT_DOWNLOAD_MEDIA = True
    
    # Rate limiting settings
    MAX_MESSAGES_PER_MINUTE = 1000
    DOWNLOAD_TIMEOUT = 30  # seconds
    
    # File size limits (in bytes)
    MAX_ATTACHMENT_SIZE = 100 * 1024 * 1024  # 100MB
    
    # Supported image formats for embedding in HTML
    IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']
    
    # Supported video formats
    VIDEO_EXTENSIONS = ['.mp4', '.mov', '.avi', '.mkv', '.webm']
    
    @classmethod
    def ensure_output_dir(cls):
        """Create output directory if it doesn't exist."""
        Path(cls.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_token(cls):
        """Check if bot token is configured."""
        if cls.BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE' or not cls.BOT_TOKEN:
            raise ValueError(
                "Bot token not configured! "
                "Set DISCORD_BOT_TOKEN environment variable or edit discord_config.py"
            )
    
    @classmethod
    def get_safe_filename(cls, filename: str) -> str:
        """Convert filename to safe filesystem name."""
        # Remove or replace unsafe characters
        unsafe_chars = '<>:"/\\|?*'
        safe_filename = filename
        for char in unsafe_chars:
            safe_filename = safe_filename.replace(char, '_')
        return safe_filename[:255]  # Limit filename length

# Environment variable examples:
# export DISCORD_BOT_TOKEN="your_actual_bot_token_here"
# export DISCORD_OUTPUT_DIR="/path/to/your/dumps"