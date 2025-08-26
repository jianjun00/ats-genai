#!/bin/bash

# Install GitHub CLI Script
# Run with: sudo ./scripts/install_github_cli.sh

set -e

echo "🚀 Installing GitHub CLI..."

# Method 1: Try snap installation (fastest)
echo "📦 Attempting snap installation..."
if command -v snap &> /dev/null; then
    snap install gh
    if command -v gh &> /dev/null; then
        echo "✅ GitHub CLI installed successfully via snap!"
        gh --version
        exit 0
    fi
fi

# Method 2: Try apt installation (most reliable)
echo "📦 Attempting apt installation..."
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /usr/share/keyrings/githubcli-archive-keyring.gpg
chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list

apt update
apt install gh -y

if command -v gh &> /dev/null; then
    echo "✅ GitHub CLI installed successfully via apt!"
    gh --version
else
    echo "❌ Installation failed. Please try manual installation."
    exit 1
fi

echo ""
echo "🎯 Next steps:"
echo "1. Authenticate with GitHub:"
echo "   gh auth login"
echo ""
echo "2. Create GitHub Issues:"
echo "   ./scripts/create_github_issues.sh"
echo ""