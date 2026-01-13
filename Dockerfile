FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create session directory
RUN mkdir -p user_sessions

# Railway uses PORT automatically (even if bot doesn't use it)
ENV PORT=8080

# Start bot
CMD ["python", "bot.py"]
