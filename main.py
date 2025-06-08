from pymongo.errors import ConnectionFailure
from rich.console import Console
from pymongo import MongoClient
import datetime
import requests
import time
import math
import sys

BASE_URL = "https://p2p.binance.com"
SEARCH_URL = "/bapi/c2c/v2/friendly/c2c/adv/search"

INTERVALO_MINUTOS = 15

HEADERS = {
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

P2P_OPTIONS = {
    "fiat": "VES",
    "asset": "USDT",
    "payTypes": ["Banesco"],
    "countries": [],
    "proMerchantAds": False,
    "shieldMerchantAds": False,
    "publisherType": None,
    "page": 1,
    "rows": 7
}

MONGO_URI = "mongodb://localhost:27017/"
MONGO_COLLECTION_NAME = "metrics_summary"
MONGO_DB_NAME = "binance_p2p"

def connect_to_mongo(uri, db_name, collection_name):
    """Conecta a MongoDB y devuelve el objeto de la colección."""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        db = client[db_name]
        
        return db[collection_name]
    except ConnectionFailure as e:
        console = Console()
        console.print(f"[bold red]Error al conectar a MongoDB: {e}[/bold red]")

        return None

def store_metrics_in_mongo(collection, metrics_doc):
    """Almacena un único documento de métricas en MongoDB."""
    if collection is None or not metrics_doc:
        return False
    try:
        result = collection.insert_one(metrics_doc)

        return result.acknowledged
    except Exception as e:
        console = Console()
        console.print(f"[bold red]Error al insertar métricas en MongoDB: {e}[/bold red]")

        return False

def get_first_page_ads(trade_type: str, trans_amount: int):
    payload = { **P2P_OPTIONS, "tradeType": trade_type, "transAmount": trans_amount }

    try:        
        response = requests.post(BASE_URL + SEARCH_URL, json=payload, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
    
        if not data.get("success"):
            return []
        
        current_ads = data.get("data")

        if not current_ads:
            return []
        
        return current_ads
    except (requests.exceptions.Timeout, requests.exceptions.RequestException):
        return []

def calc_median(numbers: list):
    if not numbers: return 0

    sorted_numbers = sorted(numbers)
    n = len(sorted_numbers)
    mid_index = n // 2

    if n % 2 == 1:
        return sorted_numbers[mid_index]
    else:
        return (sorted_numbers[mid_index - 1] + sorted_numbers[mid_index]) / 2

def calc_diff(a: float, b: float):
    if not b or b == 0: return 0
    
    return ((a - b) / b) * 100

def fetch_and_store_metrics():
    """
    Función que obtiene datos, calcula métricas y las guarda.
    """
    console = Console()

    fetch_timestamp = datetime.datetime.now(datetime.timezone.utc)
    console.print(f"[bold green]Ejecutando a las {datetime.datetime.now().strftime('%H:%M:%S')}...[/bold green]")

    buy_ads = get_first_page_ads(trade_type="BUY", trans_amount=32000)
    sell_ads = get_first_page_ads(trade_type="SELL", trans_amount=3200)

    if not buy_ads or not sell_ads:
        console.print("[yellow]No se obtuvieron datos de una o ambas operaciones. Omitiendo guardado.[/yellow]")
        
        return

    buy_prices = [float(ad['adv']['price']) for ad in buy_ads]
    buy_quantities = [float(ad['adv']['tradableQuantity']) for ad in buy_ads]
    sell_prices = [float(ad['adv']['price']) for ad in sell_ads]
    sell_quantities = [float(ad['adv']['tradableQuantity']) for ad in sell_ads]
    
    metrics_to_store = {
        "timestamp": fetch_timestamp,
        "buy_side": {
            "price_median": round(calc_median(buy_prices), 2),
            "price_min": round(min(buy_prices), 2),
            "price_max": round(max(buy_prices), 2),
            "quantity_median": round(calc_median(buy_quantities), 2),
            "volume_total": round(sum(buy_quantities), 2)
        },
        "sell_side": {
            "price_median": round(calc_median(sell_prices), 2),
            "price_min": round(min(sell_prices), 2),
            "price_max": round(max(sell_prices), 2),
            "quantity_median": round(calc_median(sell_quantities), 2),
            "volume_total": round(sum(sell_quantities), 2)
        },
        "diff": {
            "price_spread_percent": round(calc_diff(calc_median(buy_prices), calc_median(sell_prices)), 2),
            "volume_percent": round(calc_diff(sum(buy_quantities), sum(sell_quantities)), 2)
        }
    }
    
    collection = connect_to_mongo(MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME)

    if not store_metrics_in_mongo(collection, metrics_to_store):
        console.print("[red]Fallo al guardar las métricas en MongoDB.[/red]\n")

def main():
    """
    Función principal que actúa como planificador (scheduler).
    """
    console = Console()
    console.print(f"[bold green]Inicio del script a las {datetime.datetime.now().strftime('%H:%M:%S')}...[/bold green]")

    while True:
        now = datetime.datetime.now()
        seconds_into_hour = now.minute * 60 + now.second
        seconds_since_last_interval = seconds_into_hour % (INTERVALO_MINUTOS * 60)
        seconds_to_wait = (INTERVALO_MINUTOS * 60) - seconds_since_last_interval
        
        time.sleep(seconds_to_wait)
        
        try:
            fetch_and_store_metrics()
        except Exception as e:
            console.print(f"[bold red]Ocurrió un error inesperado durante la ejecución: {e}[/bold red]")
            console.print("[bold red]El scheduler continuará. Reintentando en el próximo intervalo.[/bold red]")

if __name__ == "__main__":
    main()
