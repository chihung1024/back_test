import pandas as pd
import yfinance as yf
from cachetools import cached, TTLCache
from pathlib import Path

# --- 常數設定 ---
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_FILE = DATA_DIR / "prices.parquet.gz"
JSON_FILE = DATA_DIR / "preprocessed_data.json"

# --- 快取設定 ---
cache = TTLCache(maxsize=100, ttl=900)

@cached(cache)
def load_price_data() -> pd.DataFrame:
    """
    從 Parquet 檔案載入所有價格數據。
    """
    if not PARQUET_FILE.exists():
        print(f"Warning: Price data file not found at {PARQUET_FILE}")
        return pd.DataFrame()
    try:
        df = pd.read_parquet(PARQUET_FILE)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        return df
    except Exception as e:
        print(f"Error loading price data from {PARQUET_FILE}: {e}")
        return pd.DataFrame()

@cached(cache)
def get_fundamentals() -> pd.DataFrame:
    """
    從 JSON 檔案載入基本面數據。
    """
    if not JSON_FILE.exists():
        print(f"Warning: Fundamental data file not found at {JSON_FILE}")
        return pd.DataFrame()
    try:
        return pd.read_json(JSON_FILE, orient='records')
    except Exception as e:
        print(f"Error loading fundamental data from {JSON_FILE}: {e}")
        return pd.DataFrame()

def get_all_data() -> pd.DataFrame:
    """
    獲取所有股票的價格數據。
    """
    return load_price_data()

def get_data(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    根據指定的股票代碼列表和日期範圍，從已載入的數據中篩選數據。
    """
    all_data = load_price_data()
    if all_data.empty:
        return pd.DataFrame()

    available_tickers = [t for t in tickers if t in all_data.columns]
    if not available_tickers:
        return pd.DataFrame()

    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        filtered_data = all_data.loc[start:end, available_tickers]
        return filtered_data
    except Exception as e:
        print(f"Error filtering data for tickers {tickers} from {start_date} to {end_date}: {e}")
        return pd.DataFrame()
