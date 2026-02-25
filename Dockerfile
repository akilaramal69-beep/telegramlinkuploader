FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        git \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create downloads directory
RUN mkdir -p DOWNLOADS

# Set ffmpeg path explicitly so yt-dlp always finds it
ENV FFMPEG_PATH=/usr/bin/ffmpeg

# Koyeb expects a web service to listen on port 8080
EXPOSE 8080

# Start the bot (Flask health server runs in a background thread inside bot.py)
CMD ["python3", "bot.py"]
