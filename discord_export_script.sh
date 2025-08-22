#!/bin/bash

# Discord Chat Exporter Script
# This uses the official DiscordChatExporter tool which is much safer

echo "Setting up Discord Chat Exporter..."

# Install .NET if not already installed (required for DiscordChatExporter)
if ! command -v dotnet &> /dev/null; then
    echo "Installing .NET..."
    
    # For Ubuntu/Debian
    if command -v apt-get &> /dev/null; then
        sudo apt-get update
        sudo apt-get install -y dotnet-sdk-8.0
    # For macOS
    elif command -v brew &> /dev/null; then
        brew install dotnet
    # For other systems
    else
        echo "Please install .NET manually: https://dotnet.microsoft.com/download"
        exit 1
    fi
fi

# Install DiscordChatExporter
echo "Installing DiscordChatExporter..."
dotnet tool install -g DiscordChatExporter.Cli

echo "DiscordChatExporter installed successfully!"
echo ""
echo "To get your Discord token:"
echo "1. Open Discord in browser (discord.com)"
echo "2. Press F12 → Console tab"
echo "3. Paste: (webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m=>m?.exports?.default?.getToken!==void 0).exports.default.getToken()"
echo "4. Copy the returned token (keep it secret!)"
echo ""
echo "Usage examples:"
echo ""
echo "# Export single channel to JSON:"
echo "DiscordChatExporter.Cli export -t YOUR_TOKEN -c CHANNEL_ID -f Json -o export.json"
echo ""
echo "# Export channel to HTML with media downloads:"
echo "DiscordChatExporter.Cli export -t YOUR_TOKEN -c CHANNEL_ID -f HtmlDark --media -o export/"
echo ""
echo "# Export entire guild/server:"
echo "DiscordChatExporter.Cli exportguild -t YOUR_TOKEN -g GUILD_ID -f Json -o exports/"
echo ""
echo "# Export DMs with specific user:"
echo "DiscordChatExporter.Cli exportdm -t YOUR_TOKEN -c CHANNEL_ID -f Json -o dm_export.json"
echo ""
echo "To find Channel/Guild IDs:"
echo "1. Enable Developer Mode in Discord Settings → Advanced"
echo "2. Right-click on channel/server → Copy ID"