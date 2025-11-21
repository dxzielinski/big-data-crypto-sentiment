import os
import requests
from dotenv import load_dotenv
import json
from datetime import datetime

# Wczytaj zmienne środowiskowe z pliku .env
load_dotenv()

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("Brak klucza API! Dodaj go do pliku .env jako API_KEY=...")

# Adres endpointu testowego (możesz użyć search)
url = "https://api.twitterapi.io/twitter/tweet/advanced_search"

headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

payload = {
    "query": "#SHIB lang:en -filter:retweets",
    "limit": 1,
    "include_user_data": False
}
times = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
try:
    response = requests.get(url, params=payload, headers=headers, timeout=10)
    response.raise_for_status()

    print("Połączenie działa!")
    print("Status:", response.status_code)
    data = response.json()
    print("Przykładowe dane:")
    print(data)
    
    with open(f"data/SHIB_{times}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print("Wynik zapisany do result.json")
except requests.exceptions.RequestException as e:
    print("Błąd połączenia:")
    print(e)
