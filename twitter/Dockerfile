FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PROJECT_ID="big-data-crypto-sentiment"
ENV TOPIC_ID="TwitterTopic"
# ENV SYMBOLS="SHIB,ETH,SOL,FTM"

CMD ["python", "stream_simulator_continuous.py"]
