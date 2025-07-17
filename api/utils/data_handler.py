# api/utils/data_handler.py
import os, pandas as pd, requests
from pathlib import Path
from cachetools import cached, TTLCache
from pandas.tseries.offsets import BDay

CACHE = TTLCache(maxsize=256, ttl=43200)   # 12 小時

OWNER = os.environ.get("VERCEL_GIT_REPO_OWNER", "chihung1024")
REPO  = os.environ.get("VERCEL_GIT_REPO_SLUG", "back_test")
BASE = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/data/data"

# --------------------------------------------------
# Parquet → CSV 回退
# --------------------------------------------------
@cached(CACHE)
def _read_parquet():
    """
    先嘗試讀 Parquet；若伺服器沒安裝 pyarrow / fastparquet
    或檔案讀取失敗，直接回傳 None，後續自動改讀多檔 CSV。
    """
    try:
        import pyarrow  # noqa: F401
    except ModuleNotFoundError:
        return None

    try:
        return pd.read_parquet(f"{BASE}/prices.parquet.gz")
    except Exception:
        return None

# --------------------------------------------------
# 讀取價格資料
# --------------------------------------------------
@cached(CACHE)
def read_price_data_from_repo(tickers: tuple, start: str, end: str):
    df = _read_parquet()
    if df is not None:
        out = df.loc[start:end, list(tickers)].copy()
        return out.dropna(axis=1, how="all")

    # 回退逐檔 CSV
    frames = []
    for tk in tickers:
        url = f"{BASE}/prices/{tk}.csv"
        try:
            tmp = pd.read_csv(url, index_col=0, parse_dates=True)["Close"].rename(tk)
            frames.append(tmp)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    combo = pd.concat(frames, axis=1)
    m = (combo.index >= start) & (combo.index <= end)
    return combo.loc[m]

# --------------------------------------------------
# 讀取預先處理的基本面 JSON
# --------------------------------------------------
@cached(CACHE)
def get_preprocessed_data():
    try:
        return requests.get(f"{BASE}/preprocessed_data.json", timeout=10).json()
    except Exception:
        return []

# --------------------------------------------------
# 工具：檢測資料是否缺頭
# --------------------------------------------------
def validate_data_completeness(df_raw, tickers, req_start):
    problems = []
    for tk in tickers:
        if tk in df_raw.columns:
            first = df_raw[tk].first_valid_index()
            if first is not None and first > req_start + BDay(5):
                problems.append({"ticker": tk, "start_date": first.strftime("%Y-%m-%d")})
    return problems
