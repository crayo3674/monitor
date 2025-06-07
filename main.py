from rich.console import Console
from rich.table import Table
import datetime
import requests
import time
import math
import sys

BASE_URL = "https://p2p.binance.com"
SEARCH_URL = "/bapi/c2c/v2/friendly/c2c/adv/search"

INTERVALO_MINUTOS = 10

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

    console = Console()

    table = Table(
        title=f"\nMétricas P2P Binance | USDT/VES | {', '.join(P2P_OPTIONS['payTypes'])}",
        title_style="bold magenta",
        header_style="bold blue"
    )

    table.add_column("MÉTRICA", justify="left", style="cyan")
    table.add_column("COMPRA", justify="center", style="green")
    table.add_column("VENTA", justify="center", style="red")

    table.add_row(
        "Precio Promedio (Mediana)",
        f"{calc_median(buy_prices):,.2f} VES",
        f"{calc_median(sell_prices):,.2f} VES"
    )
    table.add_row(
        "Precio Mínimo",
        f"{min(buy_prices):,.2f} VES",
        f"{min(sell_prices):,.2f} VES"
    )
    table.add_row(
        "Precio Máximo",
        f"{max(buy_prices):,.2f} VES",
        f"{max(sell_prices):,.2f} VES"
    )
    table.add_row(
        "Cantidad Promedio (Mediana)",
        f"{calc_median(buy_quantities):,.2f} USDT",
        f"{calc_median(sell_quantities):,.2f} USDT",
        end_section=True
    )
    table.add_row(
        "Volumen Total (Suma)",
        f"{sum(buy_quantities):,.2f} USDT",
        f"{sum(sell_quantities):,.2f} USDT"
    )
    
    console.print(table)

    price_diff = calc_diff(calc_median(buy_prices), calc_median(sell_prices))
    volume_diff = calc_diff(sum(buy_quantities), sum(sell_quantities))
    
    console.print(f"\n[bold yellow]Diferencia de Precio (Spread):[/bold yellow] {price_diff:.2f}%")
    console.print(f"[bold yellow]Diferencia de Volumen (Compra vs Venta):[/bold yellow] {volume_diff:.2f}%\n")

def fetch_and_display_metrics():
    """
    Función que encapsula una ejecución completa de la obtención y muestra de datos.
    """
    console = Console()
    console.print(f"[bold green]Ejecutando a las {datetime.datetime.now().strftime('%H:%M:%S')}...[/bold green]")

    buy_ads = get_first_page_ads(trade_type="BUY", trans_amount=32000)
    sell_ads = get_first_page_ads(trade_type="SELL", trans_amount=3200)
    
    process_and_display_metrics(buy_ads, sell_ads)

def main():
    """
    Función principal que actúa como planificador (scheduler).
    """
    console = Console()
    console.print("[bold cyan]Iniciando el bot de métricas P2P. Se ejecutará cada 10 minutos en punto.[/bold cyan]")
    
    while True:
        ahora = datetime.datetime.now()
        
        minutos_en_segundos = ahora.minute * 60 + ahora.second
        segundos_desde_ultimo_intervalo = minutos_en_segundos % (INTERVALO_MINUTOS * 60)
        
        segundos_para_esperar = (INTERVALO_MINUTOS * 60) - segundos_desde_ultimo_intervalo
        
        proxima_ejecucion = ahora + datetime.timedelta(seconds=segundos_para_esperar)
        
        console.print(f"Próxima ejecución a las: [bold yellow]{proxima_ejecucion.strftime('%H:%M:%S')}[/bold yellow]. Esperando {segundos_para_esperar:.0f} segundos...")
        
        time.sleep(segundos_para_esperar)
        try:
            fetch_and_display_metrics()
        except Exception as e:
            console.print(f"[bold red]Ocurrió un error inesperado durante la ejecución: {e}[/bold red]")
            console.print("[bold red]El scheduler continuará. Reintentando en el próximo intervalo.[/bold red]")

if __name__ == "__main__":
    main()
