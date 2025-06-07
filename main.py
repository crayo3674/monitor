import requests
import time
import math
import sys

BASE_URL = "https://p2p.binance.com"
SEARCH_URL = "/bapi/c2c/v2/friendly/c2c/adv/search"

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
    "rows": 20
}

def get_first_page_ads(trade_type: str, trans_amount: int):
    """
    Obtiene SÓLO LA PRIMERA PÁGINA de anuncios para un tipo de operación.
    """
    payload = {
        **P2P_OPTIONS,
        "tradeType": trade_type,
        "transAmount": trans_amount
    }
    
    try:        
        response = requests.post(
            BASE_URL + SEARCH_URL,
            json=payload,
            headers=HEADERS,
            timeout=10
        )
        
        response.raise_for_status()
        
        data = response.json()

        if not data.get("success"):
            print(f"  -> Error de API: {data.get('message', 'Error desconocido')}")
            
            return []

        current_ads = data.get("data")

        if not current_ads:
            print("  -> No se encontraron anuncios que coincidan con los criterios.")
            
            return []
        
        return current_ads

    except requests.exceptions.Timeout:
        print("  -> Error: La petición tardó demasiado en responder (timeout).")
        
        return []
    
    except requests.exceptions.RequestException as e:
        print(f"  -> Error en la petición HTTP: {e}.")
        
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

def process_and_display_metrics(buy_ads: list, sell_ads: list):
    if not buy_ads or not sell_ads:
        print("\nNo se pudieron obtener datos de una o ambas operaciones. No se pueden generar métricas completas.")
        return

    sell_prices = [float(ad['adv']['price']) for ad in sell_ads]
    sell_quantities = [float(ad['adv']['tradableQuantity']) for ad in sell_ads]
    
    buy_prices = [float(ad['adv']['price']) for ad in buy_ads]
    buy_quantities = [float(ad['adv']['tradableQuantity']) for ad in buy_ads]

    total_sell_quantity = sum(sell_quantities)
    total_buy_quantity = sum(buy_quantities)
    
    median_sell_price = calc_median(sell_prices)
    median_buy_price = calc_median(buy_prices)

    volume_diff = calc_diff(total_buy_quantity, total_sell_quantity)
    price_diff = calc_diff(median_buy_price, median_sell_price)
    
    print("\n" + "="*60)
    print(f"   Métricas P2P para USDT/VES")
    print("="*60)
    
    print("-"*60)
    
    print(f"Precio Promedio (Mediana):")
    print(f"  - COMPRA: {median_buy_price:,.2f} VES")
    print(f"  - VENTA:  {median_sell_price:,.2f} VES\n")
    
    print(f"Precio Mínimo:")
    print(f"  - COMPRA: {min(buy_prices):,.2f} VES")
    print(f"  - VENTA:  {min(sell_prices):,.2f} VES\n")

    print(f"Precio Máximo:")
    print(f"  - COMPRA: {max(buy_prices):,.2f} VES")
    print(f"  - VENTA:  {max(sell_prices):,.2f} VES\n")

    print(f"Volumen Total Disponible (USDT):")
    print(f"  - COMPRA: {total_buy_quantity:,.2f}")
    print(f"  - VENTA:  {total_sell_quantity:,.2f}\n")

    print(f"Diferencia de Precio (Spread): {price_diff:.2f}%")
    print(f"Diferencia de Volumen (Compra vs Venta): {volume_diff:.2f}%")
    print("="*60)

def main():
    print("Iniciando la obtención de datos de Binance P2P...")

    buy_ads = get_first_page_ads(trade_type="BUY", trans_amount=32000)
    sell_ads = get_first_page_ads(trade_type="SELL", trans_amount=3200)
    
    process_and_display_metrics(buy_ads, sell_ads)

if __name__ == "__main__":
    main()
