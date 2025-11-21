# coding=utf-8

import json
import time
import random
from google.cloud import pubsub_v1
import datetime
import os

# --- KONFIGURACJA GCP ---
PROJECT_ID = "big-data-crypto-sentiment"  # Zmien na ID Twojego projektu
TOPIC_ID = "TwitterTopic"

# --- sicezka DO PLIKU DANYCH ---
DATA_FILE = "data/ETH_2025-11-20_21-26-35.json"
folder_path = "data"


# --- wczytanie tweetow ---

# all_tweets = []
# for filename in os.listdir(folder_path):
#     crypto = filename[:3].upper() 
#     if filename.endswith(".json"):
#         with open(os.path.join(folder_path, filename), "r", encoding="utf-8") as f:
#             tweets = json.load(f)  # zakładamy, że plik zawiera listę tweetów
#             for tweet in tweets:
#                 pass
#                 #tweet["simulated_crypto"] = crypto
#             all_tweets.extend(tweets)

# print(f"Wczytano {len(all_tweets)} tweetów z plików JSON.")


# --- PARAMETRY SYMULACJI ---
# Czas oczekiwania między publikacją pojedynczych tweetów (w sekundach).
# Jeśli wolisz, możesz zasymulowac 'Publish every 10 seconds' dla PACZKI danych.
INTERVAL_SECONDS = 5

def publish_tweets():
    """Wczytuje tweety z JSON i publikuje je do Pub/Sub z opoznieniem."""
    
    # Inicjalizacja klienta i ścieżki tematu
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # print(f"Ładowanie danych z pliku: {DATA_FILE}")
    # try:
    #    with open(DATA_FILE, 'r', encoding='utf-8') as f:
    #        data = json.load(f)
    #        tweets = data.get('tweets', [])
    # except FileNotFoundError:
    #    print(f"Error: Plik {DATA_FILE} nie zostal znaleziony.")
    #    return
    # except json.JSONDecodeError:
    #    print(f"Error: Nieprawidlowy format JSON w pliku {DATA_FILE}.")
    #    return
    
    # print(f"Znaleziono {len(tweets)} tweetow do publikacji.")
    


    all_tweets = []
    #--- polaczenie danych z plikow jsonowych w jeden json

    for DATA_FILE in os.listdir(folder_path):
        if not DATA_FILE.endswith(".json"):
            continue

        crypto = DATA_FILE[:3].upper()  # pierwsze 3 litery jako kryptowaluta
        file_path = os.path.join(folder_path, DATA_FILE)
        
        print(f"Ładowanie danych z pliku: {DATA_FILE}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Zakładamy, że JSON może być listą tweetów lub słownikiem z kluczem 'tweets'
                if isinstance(data, dict):
                    tweets = data.get('tweets', [])
                elif isinstance(data, list):
                    tweets = data
                else:
                    print(f"Nieznany format danych w pliku {DATA_FILE}, pomijam.")
                    continue

                # Dodajemy pole simulated_crypto do każdego tweeta
                for tweet in tweets:
                    if isinstance(tweet, str):
                        tweet_dict = {"text": tweet}  # string → dict
                    else:
                        tweet_dict = tweet
                    tweet_dict["simulated_crypto"] = crypto
                    all_tweets.append(tweet_dict)

        except FileNotFoundError:
            print(f"Error: Plik {DATA_FILE} nie został znaleziony.")
            continue
        except json.JSONDecodeError:
            print(f"Error: Nieprawidłowy format JSON w pliku {DATA_FILE}.")
            continue

    print(f"Znaleziono {len(all_tweets)} tweetów do publikacji.")

    # Symulacja publikacji strumienia
    num_to_send = 3
    for i in range(num_to_send):
        tweet = random.choice(all_tweets)
        # 1. Serializacja danych
        try:
            message_data = json.dumps(tweet).encode('utf-8')
        except Exception as e:
            print(f"Błąd serializacji tweeta {i}: {e}")
            continue

        # 2.  Dodanie atrybutu czasu (timestamp)
        current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        # Wybieramy losową walutę dla ułatwienia testów
        attributes = {
            'timestamp': current_time,
            'simulated_crypto': tweet["simulated_crypto"]
        }

        # 3. Publikacja wiadomości
        future = publisher.publish(topic_path, message_data, **attributes)
        
        print(f"Opublikowano tweet {i+1}/{(num_to_send)}. ID wiadomości: {future.result()}")

        # 4. Wstrzymanie dla symulacji strumienia
        time.sleep(INTERVAL_SECONDS)

    print("\n--- Symulacja strumienia zakończona. ---")

if __name__ == '__main__':
    publish_tweets()