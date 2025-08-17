# ── update_data.py (Final Robust Version - Polite Fetching) ───────────
# 功能：
# 1.  穩定地從 iShares 官網 CSV 獲取 Russell 1000 成分股。
# 2.  自動修正股票代碼格式 (例如 BRK.B -> BRK-B)。
# 3.  採用循序分批處理，並在批次間加入延遲，以徹底解決速率限制問題。
# 4.  修正了並行處理中的 Bug，確保數據能被穩定下載。

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
MAX_WORKERS = 15
BATCH_SIZE = 100  # Process 100 tickers at a time
DELAY_BETWEEN_BATCHES = 5  # Wait 5 seconds between batches

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

def get_russell1000_constituents() -> list[str]:
    """
    直接從 iShares 官網下載 IWB ETF 持股 CSV，並修正股票代碼。
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
        
        # 【關鍵修正】自動將 'BRK.B' 轉換為 'BRK-B'
        tickers = df_stocks['Ticker'].dropna().str.replace('.', '-', regex=False).unique().tolist()
        
        print(f"✅ Successfully fetched and sanitized {len(tickers)} stock tickers.")
        return tickers
        
    except Exception as e:
        print(f"🔴 Failed to download or parse iShares holdings CSV: {e}")
        return []

def fetch_history_for_ticker(ticker):
    """下載單一股票的歷史數據。"""
    try:
        df = yf.download(ticker, start="1990-01-01", progress=False, auto_adjust=True)
        if df.empty or "Close" not in df.columns:
            return None
        df.index.name = "Date"
        df[['Close']].to_csv(PRICES_DIR / f"{ticker}.csv.gz", compression="gzip")
        return ticker
    except Exception:
        return None

def fetch_fundamentals_for_ticker(ticker):
    """下載單一股票的基本面數據。"""
    try:
        info = yf.Ticker(ticker).info
        if info and info.get("marketCap"):
            return { "ticker": ticker, "marketCap": info.get("marketCap"), "sector": info.get("sector"), "trailingPE": info.get("trailingPE"), "forwardPE": info.get("forwardPE"), "dividendYield": info.get("dividendYield") }
    except Exception:
        return None

def main():
    """主執行流程"""
    t0 = time.time()
    tickers = get_russell1000_constituents()

    if not tickers:
        print("❌ Failed to fetch constituents. Aborting update.")
        return

    all_successful_histories = []
    all_fundamentals = []
    
    ticker_batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    print(f"Starting to process {len(tickers)} tickers in {len(ticker_batches)} batches.")

    for i, batch in enumerate(ticker_batches):
        print(f"\n--- Processing Batch {i+1}/{len(ticker_batches)} ---")
        with ThreadPoolExecutor(MAX_WORKERS) as executor:
            # Fetch history
            future_hist = {executor.submit(fetch_history_for_ticker, t): t for t in batch}
            for future in tqdm(as_completed(future_hist), total=len(batch), desc="History"):
                result = future.result()
                if result:
                    all_successful_histories.append(result)
            
            # Fetch fundamentals
            future_fund = {executor.submit(fetch_fundamentals_for_ticker, t): t for t in batch}
            for future in tqdm(as_completed(future_fund), total=len(batch), desc="Fundamentals"):
                result = future.result()
                if result:
                    all_fundamentals.append(result)

        if i < len(ticker_batches) - 1:
            print(f"--- Batch {i+1} complete. Waiting for {DELAY_BETWEEN_BATCHES} seconds... ---")
            time.sleep(DELAY_BETWEEN_BATCHES)

    print(f"\n\n--- All batches processed ---")
    print(f"Fetched fundamentals for {len(all_fundamentals)} tickers.")
    print(f"Fetched price history for {len(all_successful_histories)} tickers.")

    # --- Merge Price Data ---
    frames = []
    for tk in tqdm(sorted(all_successful_histories), desc="Merging Prices"):
        file_path = PRICES_DIR / f"{tk}.csv.gz"
        if file_path.exists():
            try:
                df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
                if not df.empty:
                    frames.append(df["Close"].rename(tk))
            except Exception as e:
                print(f"Warning: Could not read or process file for {tk}. Skipping. Error: {e}")
                continue

    if frames:
        full_df = pd.concat(frames, axis=1).sort_index()
        full_df.to_parquet(PARQUET_FILE, compression="gzip")
        print(f"✅ Successfully merged {len(frames)} tickers into {PARQUET_FILE}")

    # --- Save Fundamentals Data ---
    if all_fundamentals:
        new_df = pd.DataFrame(all_fundamentals).sort_values("ticker").reset_index(drop=True)
        new_df.to_json(JSON_FILE, orient="records", indent=2)
        print(f"✅ Successfully saved fundamental data to {JSON_FILE}")
        
    print(f"✅ Data update complete. Total time: {time.time() - t0:.1f} seconds.")

if __name__ == "__main__":
    main()
