# ── update_data.py（優化完整版本）────────────────────────────
# 功能：
# 1. 先抓官方 S&P 500 / Nasdaq-100 成分；失敗改用 FMP；再失敗回 ETF / Wiki
# 2. 多執行緒下載基本面與歷史價格
# 3. 只對下載成功的股票合併 Parquet，避免 “KeyError: 'Close'”
# 4. 若基本面資料與前次相同就跳過寫檔，減少無意義 commit

import os, json, time, requests, pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf

# ─── 資料夾設定 ──────────────────────────────────────────────
DATA_DIR     = Path("data")
PRICES_DIR   = DATA_DIR / "prices"
PARQUET_FILE = DATA_DIR / "prices.parquet.gz"
JSON_FILE    = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS  = 20

DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

# ─── 1. 取得指數成分股 ───────────────────────────────────────
def sp500_official() -> list[str]:
    """直接解析 S&P 官網頁面隱藏的 indexMembers JSON。"""
    try:
        html = requests.get(
            "https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview",
            timeout=10
        ).text
        i = html.find("indexMembers")
        if i == -1:
            return []
        l = html.find("[", i)
        r = html.find("]", l) + 1
        return [m["symbol"] for m in json.loads(html[l:r])]
    except Exception:
        return []

def nasdaq_official() -> list[str]:
    """使用 Nasdaq 官方 JSON API。"""
    try:
        hdr = {"User-Agent": "Mozilla/5.0"}
        rows = requests.get(
            "https://api.nasdaq.com/api/quote/NDX/constituents",
            headers=hdr,
            timeout=10
        ).json()["data"]["rows"]
        return [r["symbol"] for r in rows]
    except Exception:
        return []

def fmp_etf_components(etf: str) -> list[str]:
    """
    以 FMP API 當備援來源：
    https://financialmodelingprep.com/api/v3/etf-holder/{etf}
    """
    key = os.getenv("FMP_TOKEN")
    if not key:
        return []
    try:
        url  = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={key}"
        rows = requests.get(url, timeout=10).json()
        return [
            row.get("symbol") or row.get("asset")
            for row in rows
            if isinstance(row, dict)
        ]
    except Exception:
        return []

def etf_holdings(etf: str) -> list[str]:
    """最後備援：直接看 ETF 成份。"""
    try:
        hold = yf.Ticker(etf).holdings
        return hold["symbol"].tolist() if hold is not None else []
    except Exception:
        return []

def wiki_sp500() -> list[str]:
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        return pd.read_html(url)[0]["Symbol"].str.replace(".", "-").tolist()
    except Exception:
        return []

def wiki_nasdaq100() -> list[str]:
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        return pd.read_html(url)[4]["Ticker"].tolist()
    except Exception:
        return []

def get_sp500() -> list[str]:
    for fn in (sp500_official, lambda: fmp_etf_components("VOO")):
        res = fn()
        if res:
            return res
    return etf_holdings("VOO") or wiki_sp500()

def get_nasdaq100() -> list[str]:
    for fn in (nasdaq_official, lambda: fmp_etf_components("QQQ")):
        res = fn()
        if res:
            return res
    return etf_holdings("QQQ") or wiki_nasdaq100()

# ─── 2. 基本面與歷史價格 ───────────────────────────────────
BASIC_EXTRA = [
    "priceToBook", "priceToSalesTrailing12Months", "ebitdaMargins",
    "grossMargins", "operatingMargins", "debtToEquity"
]

def fetch_fundamentals(ticker: str):
    """抓取單檔基本面，失敗回傳 None。"""
    try:
        info = yf.Ticker(ticker).info
        if not info.get("marketCap"):
            return None
        row = {
            "ticker": ticker,
            "marketCap": info.get("marketCap"),
            "sector": info.get("sector"),
            "trailingPE": info.get("trailingPE"),
            "forwardPE": info.get("forwardPE"),
            "dividendYield": info.get("dividendYield"),
            "returnOnEquity": info.get("returnOnEquity"),
            "revenueGrowth": info.get("revenueGrowth"),
            "earningsGrowth": info.get("earningsGrowth"),
        }
        for k in BASIC_EXTRA:
            row[k] = info.get(k)
        return row
    except Exception:
        return None

def fetch_history(ticker: str, max_retries: int = 3, pause_sec: float = 1.0):
    """
    下載單檔歷史價格。
    成功：回傳 (ticker, True) 且在 data/prices 生成 .csv.gz
    失敗：重試 max_retries 次仍無資料 → 回傳 (ticker, False)

    兩項額外優化：
    1. 將索引欄命名為 'Date'，避免後續 read_csv(index_col='Date') 時找不到欄名。
    2. 直接輸出為 gzip 壓縮檔，可將檔案體積縮小 70%–80%。
    3. 明確使用 auto_adjust=True，確保獲取的是還原股價，解決數據一致性問題。
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = yf.download(
                ticker,
                start="1990-01-01",
                progress=False,
                auto_adjust=True  # 關鍵：自動處理股票分割和股息，獲取還原股價
            )

            # 關鍵檢查：確保 df.index 是 DatetimeIndex 且有資料
            if not isinstance(df.index, pd.DatetimeIndex) or df.empty:
                raise ValueError("Invalid data: index is not DatetimeIndex or df is empty")

            # 檢查必備欄位
            if "Close" not in df.columns:
                raise ValueError("empty frame or no Close column")

            # 只保留收盤價並設定索引欄名稱
            out = df[["Close"]].copy()
            out.reset_index(inplace=True) # Make 'Date' a regular column
            out.rename(columns={'index': 'Date'}, inplace=True) # Ensure the column is named 'Date'
            out.to_csv(
                PRICES_DIR / f"{ticker}.csv.gz",
                index=False, # Do not write the index as a column
                compression="gzip"
            )
            return ticker, True

        except Exception as e:
            if attempt == max_retries:
                # 最後一次仍失敗 → 回傳 False
                return ticker, False
            # 等待後重試
            time.sleep(pause_sec)


# ─── 3. 主流程 ───────────────────────────────────────────────
def main():
    t0 = time.time()

    sp500_set  = set(get_sp500())
    ndx_set    = set(get_nasdaq100())
    tickers    = sorted(sp500_set | ndx_set)

    if not tickers:
        print("❌ 無法取得任何成份股，結束執行"); return
    print("Total symbols:", len(tickers))

    # 3-1 基本面
    fundamentals = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc="Fundamentals"):
            data = fut.result()
            if data is not None:
                fundamentals.append(data)

    for row in fundamentals:
        row["in_sp500"]     = row["ticker"] in sp500_set
        row["in_nasdaq100"] = row["ticker"] in ndx_set

    # 3-2 歷史價格
    success = set()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_history, t): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc="Prices"):
            tk, ok = fut.result()
            if ok:
                success.add(tk)

    # 從 data/prices/*.csv.gz 讀取所有成功的檔案並合併
    frames = []
    # 只讀取下載成功的股票
    price_files = [PRICES_DIR / f"{tk}.csv.gz" for tk in success]
    for f in tqdm(price_files, desc="Combining prices"):
        if f.exists():
            df = pd.read_csv(f, parse_dates=["Date"]).set_index("Date")
            # 將 Series 命名為其股票代碼
            frames.append(df["Close"].rename(f.stem.split('.')[0]))

    if frames:
        (pd.concat(frames, axis=1)
           .sort_index()
           .to_parquet(PARQUET_FILE, compression="gzip"))

    # 3-3 基本面變更偵測
    if not fundamentals:
        print("ℹ️ 無法取得任何基本面資料，跳過基本面變更偵測。")
        # Create an empty DataFrame with expected columns to avoid KeyError later
        columns = ["ticker", "marketCap", "sector", "trailingPE", "forwardPE",
                   "dividendYield", "returnOnEquity", "revenueGrowth", "earningsGrowth"] + BASIC_EXTRA
        new_df = pd.DataFrame(columns=columns)
    else:
        new_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)

    if JSON_FILE.exists():
        old_df = pd.read_json(JSON_FILE, orient="records")
        # Ensure old_df has the same columns as new_df for comparison, if new_df is empty
        if new_df.empty and not old_df.empty:
            old_df = pd.DataFrame(columns=new_df.columns) # Adjust old_df to match empty new_df structure

        if new_df.equals(old_df):
            print("ℹ️ 基本面無變動，跳過寫檔"); return

    new_df.to_json(JSON_FILE, orient="records", indent=2)
    print(f"✅ 更新完成，耗時 {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
