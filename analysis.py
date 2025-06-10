from pymongo.errors import ConnectionFailure
import matplotlib.dates as mdates
from rich.console import Console
from pymongo import MongoClient
import matplotlib.pyplot as plt
from pathlib import Path
import seaborn as sns
import pandas as pd
import datetime
import random
import pytz

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "binance_p2p"
MONGO_COLLECTION_NAME = "metrics_summary"

console = Console()

def fetch_data_from_mongo(days_ago=7):
    """
    Obtiene los datos de MongoDB de los últimos 'days_ago' días y los convierte en un DataFrame de Pandas.
    """
    try:
        console.print(f"[cyan]Conectando a MongoDB en '{MONGO_URI}'...[/cyan]")

        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        console.print("[green]Conexión exitosa.[/green]")

        caracas_tz = pytz.timezone('America/Caracas')
        start_date = datetime.datetime.now(caracas_tz) - datetime.timedelta(days=days_ago)
        query = {"timestamp": {"$gte": start_date}}
        
        console.print(f"[cyan]Obteniendo datos de los últimos {days_ago} días...[/cyan]")

        cursor = collection.find(query)
        data = list(cursor)

        if not data:
            console.print("[bold yellow]No se encontraron datos en el rango de fechas especificado.[/bold yellow]")
            
            return None
        console.print(f"[green]Se encontraron {len(data)} registros.[/green]")
        
        return pd.DataFrame(data)
    except ConnectionFailure as e:
        console.print(f"[bold red]Error de conexión con MongoDB: {e}[/bold red]")
        
        return None
    except Exception as e:
        console.print(f"[bold red]Ocurrió un error inesperado al obtener los datos: {e}[/bold red]")
        
        return None

def process_data(df):
    """
    Limpia y enriquece el DataFrame para el análisis.
    """
    if df is None:
        return None

    console.print("[cyan]Procesando y enriqueciendo los datos...[/cyan]")
    
    for side in ['buy_side', 'sell_side', 'diff']:
        flat_df = pd.json_normalize(df[side])
        flat_df = flat_df.add_prefix(f'{side}_')
        df = df.join(flat_df)

    df = df.drop(columns=['buy_side', 'sell_side', 'diff', '_id'])
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp')

    if df.index.tz is None:
            console.print("[yellow]Advertencia: Se encontraron datos sin zona horaria ('naive'). Asumiendo 'America/Caracas'...[/yellow]")
            df.index = df.index.tz_localize('America/Caracas')
        
    df['hour_of_day'] = df.index.hour
    df['day_of_week'] = df.index.day_name()
    
    console.print("[green]Procesamiento completado.[/green]")
    
    return df

def format_hour_12h(hour_24):
    """Convierte una hora en formato 24h (0-23) a un string en formato 12h AM/PM."""
    if hour_24 == 0:
        return "12 AM"
    elif hour_24 < 12:
        return f"{hour_24} AM"
    elif hour_24 == 12:
        return "12 PM"
    else:
        return f"{hour_24 - 12} PM"

def plot_price_and_spread_trends(df, output_dir):
    """
    Gráfica 1: Guarda la evolución de precios y spread en un archivo.
    """
    console.print("[cyan]Generando Gráfica 1: Evolución de Precios y Spread...[/cyan]")

    fig, ax1 = plt.subplots(figsize=(15, 7))

    color = 'tab:blue'
    ax1.set_xlabel('Fecha y Hora')
    ax1.set_ylabel('Precio (VES por USDT)', color=color)
    ax1.plot(df.index, df['buy_side_price_median'], label='Precio Mediano de Compra (Top)', color='green', alpha=0.8)
    ax1.plot(df.index, df['sell_side_price_median'], label='Precio Mediano de Venta (Top)', color='red', alpha=0.8)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax1.legend(loc='upper left')

    ax2 = ax1.twinx()
    color = 'tab:purple'
    ax2.set_ylabel('Spread de Precio (%)', color=color)
    ax2.fill_between(df.index, df['diff_price_spread_percent'], color=color, alpha=0.2, label='Spread (%)')
    ax2.tick_params(axis='y', labelcolor=color)
    
    fig.autofmt_xdate()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b %I:%M %p'))
    plt.title('Evolución de Precios P2P (USDT/VES) y Spread (%)', fontsize=16)
    fig.tight_layout()
    
    # Guardar la figura en la carpeta de salida
    plt.savefig(output_dir / '1_evolucion_precios_spread.png')
    plt.close(fig) # Cerramos la figura para liberar memoria

def plot_intraday_patterns(df, output_dir):
    """
    Gráfica 2: Guarda los patrones por hora del día en un archivo, usando formato 12h AM/PM.
    """
    console.print("[cyan]Generando Gráfica 2: Patrones por Hora del Día (Formato 12h)...[/cyan]")
    
    # Agrupar por hora (0-23)
    hourly_data = df.groupby('hour_of_day')[['diff_price_spread_percent', 'buy_side_volume_total', 'sell_side_volume_total']].mean()
    
    hourly_data.index = hourly_data.index.map(format_hour_12h)
    
    hour_order = [format_hour_12h(h) for h in range(24)]
    hourly_data = hourly_data.reindex(hour_order)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), sharex=True)
    
    # Usamos el nuevo índice con formato AM/PM para el eje x
    sns.barplot(x=hourly_data.index, y=hourly_data['diff_price_spread_percent'], ax=ax1, color='purple', alpha=0.7)
    ax1.set_title('Spread Promedio por Hora del Día', fontsize=14)
    ax1.set_ylabel('Spread Promedio (%)')
    ax1.grid(axis='y', linestyle='--', linewidth=0.5)

    hourly_data[['buy_side_volume_total', 'sell_side_volume_total']].plot(kind='bar', ax=ax2, color={'buy_side_volume_total': 'green', 'sell_side_volume_total': 'red'}, alpha=0.7)
    ax2.set_title('Volumen Total Promedio por Hora del Día', fontsize=14)
    ax2.set_ylabel('Volumen Promedio (USDT)')
    ax2.set_xlabel('Hora del Día')
    ax2.legend(['Volumen de Compra', 'Volumen de Venta'])
    ax2.grid(axis='y', linestyle='--', linewidth=0.5)

    # Rotamos las etiquetas para que no se solapen
    plt.xticks(rotation=45) 
    fig.tight_layout()
    
    # Guardar la figura en la carpeta de salida
    plt.savefig(output_dir / '2_patrones_horarios.png')
    plt.close(fig) # Cerramos la figura para liberar memoria

def plot_day_of_week_patterns(df, output_dir):
    """
    Gráfica 3: Guarda los patrones por día de la semana en un archivo.
    """
    console.print("[cyan]Generando Gráfica 3: Patrones por Día de la Semana...[/cyan]")
    
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    daily_data = df.groupby('day_of_week')['diff_price_spread_percent'].mean().reindex(day_order)
    
    fig = plt.figure(figsize=(12, 6)) # Capturamos la figura para poder cerrarla
    
    sns.barplot(x=daily_data.index, y=daily_data.values, palette='viridis', hue=daily_data.index, legend=False)
    plt.title('Spread Promedio por Día de la Semana', fontsize=16)
    plt.xlabel('Día de la Semana')
    plt.ylabel('Spread Promedio (%)')
    plt.grid(axis='y', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # Guardar la figura en la carpeta de salida
    plt.savefig(output_dir / '3_patrones_semanales.png')
    plt.close(fig) # Cerramos la figura para liberar memoria

def main():
    """
    Función principal que orquesta la obtención, procesamiento y guardado de gráficas.
    """
    console.print("[bold green]--- Inicio del Script de Análisis P2P ---[/bold green]")
    
    df_raw = fetch_data_from_mongo(days_ago=7)
    
    if df_raw is None:
        console.print("[bold red]No se pudo continuar con el análisis. Saliendo del script.[/bold red]")
        return
        
    df_processed = process_data(df_raw)

    if df_processed is None:
        console.print("[bold red]El procesamiento de datos falló. Saliendo del script.[/bold red]")
        return
    
    log_base_dir = Path("log")

    today_str = datetime.date.today().strftime("%Y-%m-%d")
    random_id = f"{random.randint(0, 999):03d}"
    analysis_folder_name = f"analysis-{today_str}-{random_id}"

    output_dir = log_base_dir / analysis_folder_name

    # 'parents=True' crea la carpeta 'log' si no existe. 'exist_ok=True' evita errores.
    output_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"[cyan]Creada carpeta de salida: [bold]'{output_dir}'[/bold][/cyan]")

    # Configuración de estilo para las gráficas
    sns.set_theme(style="whitegrid")
    plt.style.use('seaborn-v0_8-darkgrid')
    
    # Generar y guardar todas las gráficas en la nueva carpeta
    plot_price_and_spread_trends(df_processed, output_dir)
    plot_intraday_patterns(df_processed, output_dir)
    plot_day_of_week_patterns(df_processed, output_dir)
    
    console.print("[bold green]\n--- Fin del Análisis ---[/bold green]")
    console.print(f"Las gráficas han sido guardadas exitosamente en la carpeta: [bold magenta]{output_dir}[/bold magenta]")

if __name__ == "__main__":
    main()
