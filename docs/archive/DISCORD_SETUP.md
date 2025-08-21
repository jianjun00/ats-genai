# Discord Content Dumper Bot Setup

This bot can dump Discord channel content including messages, images, and attachments to various formats (JSON, CSV, HTML).

## Setup Steps

### 1. Create Discord Application & Bot

1. Go to https://discord.com/developers/applications
2. Click "New Application" and give it a name
3. Go to the "Bot" section
4. Click "Add Bot"
5. Copy the bot token (keep this secret!)

### 2. Install Dependencies

```bash
# Install Python dependencies
pip install -r discord_requirements.txt

# Or using uv (if available)
uv pip install -r discord_requirements.txt
```

### 3. Configure Bot Token

**Option A: Environment Variable (Recommended)**
```bash
export DISCORD_BOT_TOKEN="your_actual_bot_token_here"
export DISCORD_OUTPUT_DIR="./discord_dumps"  # Optional
```

**Option B: Edit Config File**
Edit `discord_config.py` and replace `YOUR_BOT_TOKEN_HERE` with your actual token.

### 4. Invite Bot to Your Server

1. In Discord Developer Portal, go to OAuth2 → URL Generator
2. Select scopes: `bot`
3. Select bot permissions:
   - Read Messages/View Channels
   - Read Message History
   - Send Messages
   - Attach Files
4. Copy the generated URL and open it to invite the bot

### 5. Run the Bot

```bash
python discord_dumper_bot.py YOUR_BOT_TOKEN
```

Or if using environment variables:
```bash
python discord_dumper_bot.py $DISCORD_BOT_TOKEN
```

## Bot Commands

### Dump Single Channel
```
!dumpchannel [channel_id] [limit] [format] [download_media]
```

**Examples:**
- `!dumpchannel` - Dump current channel as JSON with media
- `!dumpchannel 123456789` - Dump specific channel by ID
- `!dumpchannel 123456789 1000 html true` - Dump 1000 messages as HTML
- `!dumpchannel 123456789 500 csv false` - Dump 500 messages as CSV without media

### Dump Entire Server
```
!dumpguild [guild_id] [limit]
```

**Examples:**
- `!dumpguild` - Dump all channels in current server
- `!dumpguild 987654321 1000` - Dump specific server with 1000 messages per channel

### Get Help
```
!dumphelp
```

## Output Formats

### JSON Format
- Complete message data with metadata
- Includes attachments, embeds, reactions
- Best for data analysis

### CSV Format
- Tabular format for spreadsheet analysis
- Basic message info only
- Good for statistics

### HTML Format
- Human-readable format
- Embedded images display inline
- Best for archival/reading

## File Structure

After dumping, you'll get:
```
discord_dumps/
├── ServerName_ChannelName_20240101_120000.json
├── ServerName_ChannelName_20240101_120000.csv
├── ServerName_ChannelName_20240101_120000.html
└── media_ServerName_ChannelName/
    ├── 12345_image1.jpg
    ├── 67890_document.pdf
    └── ...
```

## Advanced Usage

### Using .env File
Create a `.env` file:
```
DISCORD_BOT_TOKEN=your_token_here
DISCORD_OUTPUT_DIR=/path/to/dumps
```

Then install python-dotenv and load it:
```python
from dotenv import load_dotenv
load_dotenv()
```

### Batch Processing Script
```python
import asyncio
from discord_dumper_bot import DiscordDumper

async def batch_dump():
    bot = DiscordDumper()
    
    # List of channel IDs to dump
    channels = [123456789, 987654321, 555666777]
    
    for channel_id in channels:
        channel = bot.get_channel(channel_id)
        if channel:
            messages = await bot.dump_channel_messages(channel)
            bot.save_as_json(messages, f"batch_{channel.name}")

# Run batch dump
# asyncio.run(batch_dump())
```

## Security Notes

- **Never commit your bot token to version control**
- Add `discord_config.py` to `.gitignore` if you store tokens there
- Use environment variables in production
- Bot needs appropriate permissions for each channel
- Some channels may be restricted and will be skipped

## Troubleshooting

### Bot Can't See Messages
- Check bot permissions in Discord server settings
- Ensure bot role is above channels you want to access
- Bot needs "Read Message History" permission

### Rate Limiting
- Discord has rate limits - bot will automatically handle them
- Large dumps may take time
- Consider using smaller limits for testing

### Large File Handling
- Very large attachments may timeout
- Adjust `DOWNLOAD_TIMEOUT` in config if needed
- Consider downloading media separately for large dumps

### Permission Errors
- Bot needs write permissions to output directory
- Check file system permissions if dumps fail to save

## Legal & Ethical Considerations

- Only dump content you have permission to access
- Respect Discord's Terms of Service
- Be mindful of privacy when sharing dumps
- Consider data retention policies for sensitive content