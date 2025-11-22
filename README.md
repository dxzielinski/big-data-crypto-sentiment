# big-data-crypto-sentiment

Big Data Project

Terraform workflow:

```bash
terraform init -upgrade
terraform fmt
terraform validate
terraform apply -var="project_id=<PROJECT_ID>"
```

Pushing docker image to gcr:

```bash
cd  ~/big-data-crypto-sentiment/coincap-simulation
docker build --no-cache -t europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/crypto-simulation .
docker push europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/crypto-simulation

cd  ~/big-data-crypto-sentiment/coincap
docker build --no-cache -t europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/coincap .
docker push europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/coincap

cd  ~/big-data-crypto-sentiment/twitter
docker build --no-cache -t europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/twitter-simulation .
docker push europe-central2-docker.pkg.dev/big-data-crypto-sentiment/big-data-crypto-sentiment-repo/twitter-simulation
```

To access vm:

```bash
gcloud compute ssh big-data-crypto-vm --zone=europe-central2-a
# logs from startup script:
sudo journalctl -u google-startup-scripts.service --no-pager | grep startup-script
sudo docker logs crypto-simulation --tail 200
# to re-run startup script:
sudo systemctl restart google-startup-scripts.service
# to stop docker manually:
sudo docker stop crypto-simulation
exit
```

To stop vm:

```bash
gcloud compute instances stop big-data-crypto-vm --zone=europe-central2-a
```

## Coincap API

Price API call

```bash
curl -X 'GET' \
  'https://rest.coincap.io/v3/price/bysymbol/BTC%2CETH%2CSOL%2CXRP%2CADA' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer API_KEY_HERE'
```

Gives example response:

```json
{
  "timestamp": 1763753634750,
  "data": [
    "84739.050000000002910383",
    "2770.139999999999872671",
    "129.180000000000006821",
    "1.959917727651328656",
    "0.412200000000000011"
  ]
}
```

Technical Analysis API call (it works only for single asset at a time)

```bash
curl -X 'GET' \
  'https://rest.coincap.io/v3/ta/bitcoin/allLatest?fetchInterval=h1' \
  -H 'accept: */*' \
  -H 'Authorization: Bearer API_KEY_HERE'
```

Gives example response:

```json
{
  "ema": {
    "ema": 84403.28849129402,
    "time": 1763751608753,
    "date": "2025-11-21T19:00:08.753Z"
  },
  "sma": {
    "sma": 90822.12006855608,
    "time": 1763751608753,
    "date": "2025-11-21T19:00:08.753Z"
  },
  "rsi": {
    "rsi": 55.31218060178186,
    "time": 1763751608753,
    "date": "2025-11-21T19:00:08.753Z"
  },
  "macd": {
    "macd": -1170.068547606693,
    "signal": -1473.022897999198,
    "histogram": 302.954350392505,
    "time": 1763751608753,
    "date": "2025-11-21T19:00:08.753Z"
  },
  "vwap24": 84863.86987084257,
  "timestamp": 1763753541354
}
```

But the app gives slightly transformed results.

For price:

```json
    {
      "timestamp": ms,
      "ETH": float,
      "SOL": float,
      "SHIB": float
    }
```

For TA:

```json
    {
        "timestamp": ms,
        "symbol": "ETH",
        "sma": float,
        "rsi": float,
        "macd": float,
        "macd_signal": float,
        "macd_hist": float,
        "vwap24": float,
        "time": ms,
        "date": "ISO8601 UTC"
    }
```