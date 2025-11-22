# Use official Python slim image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install gcc (if needed by some packages)
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV PROJECT_ID="big-data-crypto-sentiment"
ENV TOPIC_ID="TwitterTopic"
# ENV SYMBOLS="SHIB,ETH,SOL,FTM"

# Entry point
CMD ["python", "stream_simulator_continuous.py"]
