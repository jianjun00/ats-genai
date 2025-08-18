#!/bin/bash

# Setup DiscordChatExporter for bulk channel dumping
set -e

echo "🚀 Setting up DiscordChatExporter for bulk channel export..."

# Check if .NET is installed
if ! command -v dotnet &> /dev/null; then
    echo "📦 Installing .NET SDK..."
    
    # Detect OS and install .NET
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Ubuntu/Debian
        if command -v apt-get &> /dev/null; then
            wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
            sudo dpkg -i packages-microsoft-prod.deb
            rm packages-microsoft-prod.deb
            sudo apt-get update
            sudo apt-get install -y dotnet-sdk-8.0
        # CentOS/RHEL/Fedora
        elif command -v yum &> /dev/null || command -v dnf &> /dev/null; then
            sudo rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
            if command -v dnf &> /dev/null; then
                sudo dnf install -y dotnet-sdk-8.0
            else
                sudo yum install -y dotnet-sdk-8.0
            fi
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install dotnet
        else
            echo "❌ Please install Homebrew first: https://brew.sh/"
            exit 1
        fi
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        # Windows (Git Bash/WSL)
        echo "🪟 Please install .NET manually from: https://dotnet.microsoft.com/download"
        echo "Then run this script again."
        exit 1
    fi
else
    echo "✅ .NET SDK already installed"
fi

# Install DiscordChatExporter
echo "📥 Installing DiscordChatExporter..."
dotnet tool install -g DiscordChatExporter.Cli --version 2.43.1

# Verify installation
if command -v DiscordChatExporter.Cli &> /dev/null; then
    echo "✅ DiscordChatExporter installed successfully!"
    echo "📍 Version: $(DiscordChatExporter.Cli --version)"
else
    echo "❌ Installation failed. Trying alternative installation..."
    
    # Alternative: download directly
    mkdir -p ~/discord-exporter
    cd ~/discord-exporter
    
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        wget https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.linux-x64.zip
        unzip DiscordChatExporter.Cli.linux-x64.zip
        chmod +x DiscordChatExporter.Cli
        export PATH="$PATH:$(pwd)"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        wget https://github.com/Tyrrrz/DiscordChatExporter/releases/latest/download/DiscordChatExporter.Cli.osx-x64.zip
        unzip DiscordChatExporter.Cli.osx-x64.zip
        chmod +x DiscordChatExporter.Cli
        export PATH="$PATH:$(pwd)"
    fi
    
    echo "✅ DiscordChatExporter downloaded to ~/discord-exporter"
fi

# Create export directory
mkdir -p discord_exports
echo "📁 Created export directory: discord_exports/"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Get your Discord token (see instructions below)"
echo "2. Run: ./bulk_discord_export.py YOUR_TOKEN"
echo ""
echo "🔑 How to get your Discord token:"
echo "1. Open Discord in browser (discord.com)"
echo "2. Press F12 → Console tab"
echo "3. Paste and run:"
echo "   (webpackChunkdiscord_app.push([[''],{},e=>{m=[];for(let c in e.c)m.push(e.c[c])}]),m).find(m=>m?.exports?.default?.getToken!==void 0).exports.default.getToken()"
echo "4. Copy the returned token (keep it secret!)"
echo ""
echo "⚠️  IMPORTANT: Never share your token with anyone!"