#!/bin/bash

# FFmpeg Installation Script for Ubuntu/Debian
# This script installs FFmpeg and Aria2, which are required for the bot to function correctly.

echo "🚀 Starting FFmpeg and Aria2 installation..."

# Update package list
sudo apt-get update

# Install FFmpeg and Aria2
sudo apt-get install -y ffmpeg aria2

# Verify installation
if command -v ffmpeg >/dev/null 2>&1; then
    echo "✅ FFmpeg installed successfully: $(ffmpeg -version | head -n 1)"
else
    echo "❌ FFmpeg installation failed. Please install it manually."
    exit 1
fi

if command -v aria2c >/dev/null 2>&1; then
    echo "✅ Aria2 installed successfully: $(aria2c --version | head -n 1)"
else
    echo "❌ Aria2 installation failed. Please install it manually."
    exit 1
fi

echo "🎉 Installation complete! You can now start your bot."
