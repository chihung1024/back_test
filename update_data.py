# ── update_data.py (最終穩定版 - 直接下載官方 CSV) ─────────────
# 功能：
# 1. 透過直接下載 iShares 官網的 CSV 檔案，穩定獲取 IWB (Russell 1000 ETF) 的持股。
# 2. 多執行緒並行下載歷史價格與基本面數據。
# 3. 將所有股價合併為單一、高效的 Parquet 檔案。

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
MAX_WORKERS = 20

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

def get_russell1000_constituents_from_ishares() -> list[str]:
    """
    直接從 iShares (BlackRock) 官網下載 IWB ETF 的持股 CSV 檔案。
    這是最穩定和官方的方法。
    """
    try:
        # 偽裝成瀏覽器以避免被阻擋
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # iShares 提供的 IWB 持股 CSV 下載連結
        url = "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # 如果請求失敗則拋出錯誤

        # 讀取 CSV 內容，跳過 iShares CSV 檔案開頭的說明文字
        # 我們透過尋找 "Ticker" 這個詞來確定表格的起始位置
        content = response.text
        if 'Ticker' not in content:
            raise ValueError("CSV content does not contain 'Ticker' header.")

        # 將從 "Ticker" 開始的內容讀入 pandas DataFrame
        csv_data = StringIO(content[content.find('Ticker'):])
        df = pd.read_csv(csv_data)
        
        # 篩選掉現金等非股票資產
        df_stocks = df[df['Asset Class'] == 'Equity'].copy()
        
        tickers = df_stocks['Ticker'].dropna().unique().tolist()
        
        print(f"✅ Successfully fetched {len(tickers)} stock tickers from iShares official CSV.")
        return tickers
        
    except Exception as e:
        print(f"🔴 Failed to download or parse iShares holdings CSV: {e}")
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

    tickers = get_russell1000_constituents_from_ishares()

    if not tickers:
        print("❌ Failed to fetch constituents. Aborting update.")
        return

    # --- 後續流程不變 ---
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

    if fundamentals:
        new_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)
        new_df.to_json(JSON_FILE, orient="records", indent=2)
        print(f"Successfully saved fundamental data to {JSON_FILE}")
        
    print(f"✅ Data update complete. Total time: {time.time() - t0:.1f} seconds.")

if __name__ == "__main__":
    main()
