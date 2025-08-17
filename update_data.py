# ── update_data.py (Final, Syntactically Correct Version) ─────────────
# 功能：
# 1. 透過直接下載 iShares 官網的 CSV 檔案，穩定獲取 IWB (Russell 1000 ETF) 的持股。
# 2. 採用分批次下載來避免 yfinance 的速率限制 (Rate Limiting)。
# 3. 修正了 Python 語法錯誤並增強了錯誤處理。

import os
import json
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf
import requests
from io import StringIO

# --- Configuration ---
DATA_DIR = Path("data")
PRICES_DIR = DATA_DIR / "prices"
PARQUET_FILE = DATA_DIR / "prices.parquet.gz"
JSON_FILE = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS = 10  # Reduced workers to be less aggressive
BATCH_SIZE = 100  # Process tickers in batches

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

def get_russell1000_constituents_from_ishares() -> list[str]:
    """
    直接從 iShares (BlackRock) 官網下載 IWB ETF 的持股 CSV 檔案。
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        url = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        content = response.text
        if 'Ticker' not in content:
            raise ValueError("CSV content does not contain 'Ticker' header.")

        csv_data = StringIO(content[content.find('Ticker'):])
        df = pd.read_csv(csv_data)
        
        df_stocks = df[df['Asset Class'] == 'Equity'].copy()
        tickers = df_stocks['Ticker'].dropna().unique().tolist()
        
        print(f"✅ Successfully fetched {len(tickers)} stock tickers from iShares official CSV.")
        return tickers
        
    except Exception as e:
        print(f"🔴 Failed to download or parse iShares holdings CSV: {e}")
        return []

def fetch_and_save_data_batch(tickers: list[str]):
    """
    為一批股票下載歷史數據和基本面數據。
    使用 yf.download 和 yf.Tickers 進行更高效的批次請求。
    """
    successful_histories = []
    fundamentals = []
    
    # --- Fetch History Data ---
    try:
        data = yf.download(tickers, start="1990-01-01", progress=False, auto_adjust=True, group_by='ticker')
        for ticker in tickers:
            if ticker in data and not data[ticker].empty:
                df_hist = data[ticker][['Close']].copy()
                if not df_hist.empty:
                    df_hist.index.name = "Date"
                    df_hist.to_csv(PRICES_DIR / f"{ticker}.csv.gz", compression="gzip")
                    successful_histories.append(ticker)
    except Exception as e:
        print(f"Warning: Batch history download failed for {len(tickers)} tickers. Error: {e}")

    # --- Fetch Fundamental Data ---
    try:
        ticker_objects = yf.Tickers(tickers)
        for ticker_str in tickers:
            try:
                info = ticker_objects.tickers[ticker_str].info
                if info and info.get("marketCap"):
                    fundamentals.append({
                        "ticker": info.get("symbol"), 
                        "marketCap": info.get("marketCap"), 
                        "sector": info.get("sector"), 
                        "trailingPE": info.get("trailingPE"), 
                        "forwardPE": info.get("forwardPE"), 
                        "dividendYield": info.get("dividendYield")
                    })
            except Exception:
                continue # Skip if single ticker info fails
    except Exception as e:
        print(f"Warning: Batch fundamental download failed. Error: {e}")
    
    return successful_histories, fundamentals

def main():
    """主執行流程"""
    t0 = time.time()
    tickers = get_russell1000_constituents_from_ishares()

    if not tickers:
        print("❌ Failed to fetch constituents. Aborting update.")
        return

    all_successful_histories = []
    all_fundamentals = []

    ticker_batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(fetch_and_save_data_batch, batch): batch for batch in ticker_batches}
        
        for future in tqdm(as_completed(future_to_batch), total=len(ticker_batches), desc="Processing batches"):
            try:
                histories, fundamentals = future.result()
                all_successful_histories.extend(histories)
                all_fundamentals.extend(fundamentals)
            except Exception as e:
                print(f"Error processing a batch: {e}")

    print(f"\nFetched fundamentals for {len(all_fundamentals)} tickers.")
    print(f"Fetched price history for {len(all_successful_histories)} tickers.")

    frames = []
    for tk in tqdm(sorted(all_successful_histories), desc="Merging prices"):
        file_path = PRICES_DIR / f"{tk}.csv.gz"
        if file_path.exists():
            try:
                df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
                if not df.empty:
                    frames.append(df["Close"].rename(tk))
            except Exception as e:
                print(f"Could not read or process file for {tk}. Skipping. Error: {e}")
                continue

    if frames:
        full_df = pd.concat(frames, axis=1).sort_index()
        full_df.to_parquet(PARQUET_FILE, compression="gzip")
        print(f"Successfully merged {len(frames)} tickers into {PARQUET_FILE}")

    if all_fundamentals:
        new_df = pd.DataFrame(all_fundamentals).sort_values("ticker").reset_index(drop=True)
        new_df.to_json(JSON_FILE, orient="records", indent=2)
        print(f"Successfully saved fundamental data to {JSON_FILE}")
        
    print(f"✅ Data update complete. Total time: {time.time() - t0:.1f} seconds.")

if __name__ == "__main__":
    main()
