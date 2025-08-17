# ── update_data.py (Robust, ETF-only version) ───────────────────
# 功能：
# 1. 直接且穩定地抓取 IWB (Russell 1000 ETF) 的持股作為成分股來源
# 2. 多執行緒並行下載歷史價格與基本面數據
# 3. 將所有股價合併為單一、高效的 Parquet 檔案

import os
import json
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf

# --- Configuration ---
DATA_DIR = Path("data")
PRICES_DIR = DATA_DIR / "prices"
PARQUET_FILE = DATA_DIR / "prices.parquet.gz"
JSON_FILE = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS = 20

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

def get_russell1000_constituents() -> list[str]:
    """
    透過抓取 IWB (iShares Russell 1000 ETF) 的持股來獲取成分股列表。
    這是目前最穩定可靠的方法。
    """
    try:
        iwb = yf.Ticker("IWB")
        # 【關鍵修正】使用 .constituents 而不是 .holdings
        holdings = iwb.constituents
        if holdings is not None and not holdings.empty:
            # The tickers are in the index of the returned DataFrame
            tickers = holdings.index.tolist()
            print(f"✅ Successfully fetched {len(tickers)} tickers from IWB ETF constituents.")
            # yfinance already provides clean tickers, no need to replace dots
            return tickers
        print("🔴 Fetched constituents data is empty.")
        return []
    except Exception as e:
        print(f"🔴 ETF constituents fetch failed: {e}.")
        return []

def fetch_fundamentals(ticker: str):
    """抓取單檔基本面數據。"""
    try:
        info = yf.Ticker(ticker).info
        if not info.get("marketCap"): return None
        return { "ticker": ticker, "marketCap": info.get("marketCap"), "sector": info.get("sector"), "trailingPE": info.get("trailingPE"), "forwardPE": info.get("forwardPE"), "dividendYield": info.get("dividendYield"), "returnOnEquity": info.get("returnOnEquity"), "revenueGrowth": info.get("revenueGrowth"), "earningsGrowth": info.get("earningsGrowth"), "priceToBook": info.get("priceToBook"), "priceToSalesTrailing12Months": info.get("priceToSalesTrailing12Months"), "operatingMargins": info.get("operatingMargins"), }
    except Exception:
        return None

def fetch_history(ticker: str, max_retries: int = 3, pause_sec: float = 1.0):
    """下載單檔股票的歷史價格並存為壓縮 CSV。"""
    for _ in range(max_retries):
        try:
            df = yf.download(ticker, start="1990-01-01", progress=False, auto_adjust=True)
            if df.empty or "Close" not in df.columns: raise ValueError("Empty data")
            out = df[["Close"]].copy()
            out.index.name = "Date"
            out.to_csv(PRICES_DIR / f"{ticker}.csv.gz", compression="gzip")
            return ticker, True
        except Exception:
            time.sleep(pause_sec)
    return ticker, False

def main():
    """主執行流程"""
    t0 = time.time()

    tickers = get_russell1000_constituents()

    if not tickers:
        print("❌ Failed to fetch constituents. Aborting update.")
        return

    # --- 1. Fetch Fundamentals and Price History in Parallel ---
    fundamentals, successful_tickers = [], set()
    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(fetch_fundamentals, t): t for t in tickers}
        future_to_ticker.update({executor.submit(fetch_history, t): t for t in tickers})
        for future in tqdm(as_completed(future_to_ticker), total=len(future_to_ticker), desc="Fetching data"):
            result = future.result()
            if isinstance(result, dict) and result:
                fundamentals.append(result)
            elif isinstance(result, tuple) and result[1]:
                successful_tickers.add(result[0])

    print(f"\nFetched fundamentals for {len(fundamentals)} tickers.")
    print(f"Fetched price history for {len(successful_tickers)} tickers.")

    # --- 2. Merge Price Data into a single Parquet file ---
    frames = []
    for tk in tqdm(sorted(list(successful_tickers)), desc="Merging prices"):
        file_path = PRICES_DIR / f"{tk}.csv.gz"
        if file_path.exists():
            df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
            if not df.empty:
                frames.append(df["Close"].rename(tk))

    if frames:
        full_df = pd.concat(frames, axis=1).sort_index()
        full_df.to_parquet(PARQUET_FILE, compression="gzip")
        print(f"Successfully merged {len(frames)} tickers into {PARQUET_FILE}")

    # --- 3. Save Fundamentals Data ---
    if fundamentals:
        new_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)
        new_df.to_json(JSON_FILE, orient="records", indent=2)
        print(f"Successfully saved fundamental data to {JSON_FILE}")
        
    print(f"✅ Data update complete. Total time: {time.time() - t0:.1f} seconds.")

if __name__ == "__main__":
    main()
