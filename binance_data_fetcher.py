import requests
import time
import csv
from datetime import datetime, timezone

BINANCE_BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"
MAX_LIMIT = 1000  

def to_milliseconds(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def from_milliseconds(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def fetch_klines(symbol: str, interval: str, start_time_ms: int, end_time_ms: int) -> list:
    all_klines = []
    current_start = start_time_ms

    while current_start < end_time_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": current_start,
            "endTime": end_time_ms,
            "limit": MAX_LIMIT
        }

        resp = requests.get(BINANCE_BASE_URL + KLINES_ENDPOINT, params=params, timeout=10)
        resp.raise_for_status()
        klines = resp.json()

        if not klines:
            break

        all_klines.extend(klines)

        last_open_time = klines[-1][0]

        current_start = last_open_time + 60_000

        time.sleep(0.1)

    return all_klines


def save_klines_to_csv(klines: list, filename: str):
    header = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore"
    ]

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for k in klines:
            writer.writerow(k)

def download_1m_klines(symbol: str, start_dt: datetime, end_dt: datetime, output_csv: str):
    
    interval = "1m"

    start_ms = to_milliseconds(start_dt)
    end_ms = to_milliseconds(end_dt)

    if start_ms >= end_ms:
        raise ValueError("start_dt muss vor end_dt liegen.")

    print(f"Hole 1m-Klines für {symbol} von {start_dt} bis {end_dt} (UTC)...")
    klines = fetch_klines(symbol, interval, start_ms, end_ms)
    print(f"Anzahl geladener Candles: {len(klines)}")

    if not klines:
        print("Keine Daten im angegebenen Zeitraum gefunden.")
        return

    save_klines_to_csv(klines, output_csv)
    print(f"Daten gespeichert in: {output_csv}")

    first_open = from_milliseconds(klines[0][0])
    last_open = from_milliseconds(klines[-1][0])
    print(f"Zeitraum der gespeicherten Daten: {first_open} bis {last_open}")

if __name__ == "__main__":
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    download_1m_klines(
        symbol="BTCUSDT",
        start_dt=start,
        end_dt=end,
        output_csv="btcusdt_1m_2024-01-01.csv"
    )
