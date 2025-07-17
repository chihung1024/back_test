#!/usr/bin/env python3
# update_data.py
# 完整版：2025-07-17
import os, json, time, requests, pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf

# --------------------------------------------------
# 全域設定
# --------------------------------------------------
DATA_DIR     = Path("data")
PRICES_DIR   = DATA_DIR / "prices"
PARQUET_PATH = DATA_DIR / "prices.parquet.gz"
JSON_PATH    = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS  = 20

DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# 1. 指數成分股 ── A:官網 → B:FMP → C:ETF/Wiki
# --------------------------------------------------
def fetch_sp500_official():
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

def fetch_nasdaq100_official():
    try:
        url = "https://api.nasdaq.com/api/quote/NDX/constituents"
        hdr = {"User-Agent": "Mozilla/5.0"}
        rows = requests.get(url, headers=hdr, timeout=10).json()["data"]["rows"]
        return [r["symbol"] for r in rows]
    except Exception:
        return []

def fetch_index_from_fmp(etf):
    token = os.getenv("FMP_TOKEN")
    if not token:
        return []
    try:
        url  = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={token}"
        rows = requests.get(url, timeout=10).json()
        out  = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r.get("symbol") or r.get("asset"))
        return [t for t in out if t]
    except Exception:
        return []

def get_sp500_list():
    for fn in (
        fetch_sp500_official,
        lambda: fetch_index_from_fmp("VOO"),
    ):
        tickers = fn()
        if tickers:
            return tickers
    return get_etf_holdings("VOO") or get_sp500_from_wiki()

def get_nasdaq100_list():
    for fn in (
        fetch_nasdaq100_official,
        lambda: fetch_index_from_fmp("QQQ"),
    ):
        tickers = fn()
        if tickers:
            return tickers
    return get_etf_holdings("QQQ") or get_nasdaq100_from_wiki()

# --------------------------------------------------
# 2. 備援方法（ETF / Wikipedia）
# --------------------------------------------------
def get_etf_holdings(etf):
    try:
        h = yf.Ticker(etf).holdings
        return h["symbol"].tolist() if h is not None else []
    except Exception:
        return []

def get_sp500_from_wiki():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        return (
            pd.read_html(url)[0]["Symbol"]
            .str.replace(".", "-", regex=False)
            .tolist()
        )
    except Exception:
        return []

def get_nasdaq100_from_wiki():
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        return pd.read_html(url)[4]["Ticker"].tolist()
    except Exception:
        return []

# --------------------------------------------------
# 3. 基本面與歷史價格
# --------------------------------------------------
EXTRA = [
    "priceToBook",
    "priceToSalesTrailing12Months",
    "ebitdaMargins",
    "grossMargins",
    "operatingMargins",
    "debtToEquity",
]

def fetch_fundamentals(tk: str):
    try:
        info = yf.Ticker(tk).info
        if not info.get("marketCap"):
            return None
        d = {
            "ticker": tk,
            "marketCap": info.get("marketCap"),
            "sector": info.get("sector"),
            "trailingPE": info.get("trailingPE"),
            "forwardPE": info.get("forwardPE"),
            "dividendYield": info.get("dividendYield"),
            "returnOnEquity": info.get("returnOnEquity"),
            "revenueGrowth": info.get("revenueGrowth"),
            "earningsGrowth": info.get("earningsGrowth"),
        }
        for k in EXTRA:
            d[k] = info.get(k)
        return d
    except Exception:
        return None

def fetch_price_history(tk: str):
    """
    下載單檔價格，若成功回傳 (ticker, True)，失敗回 (ticker, False)
    """
    try:
        df = yf.download(
            tk,
            start="1990-01-01",
            progress=False,
            auto_adjust=True,
        )[["Close"]]
        if df.empty:
            return tk, False
        df.index.name = "Date"
        df.to_csv(PRICES_DIR / f"{tk}.csv", index_label="Date")
        return tk, True
    except Exception:
        return tk, False

# --------------------------------------------------
# 4. 主流程
# --------------------------------------------------
def main():
    t0 = time.time()

    sp  = set(get_sp500_list())
    nd  = set(get_nasdaq100_list())
    all_tickers = sorted(sp | nd)
    if not all_tickers:
        print("❌ 取不到任何成分股，流程終止")
        return
    print("Total symbols:", len(all_tickers))

    # 4-1 基本面
    fundamentals = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_fundamentals, t): t for t in all_tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Fundamentals"):
            d = f.result()
            if d:
                fundamentals.append(d)

    for d in fundamentals:
        d["in_sp500"]     = d["ticker"] in sp
        d["in_nasdaq100"] = d["ticker"] in nd

    # 4-2 價格
    success = set()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_price_history, t): t for t in all_tickers}
        for f in tqdm(as_completed(futs), total=len(futs), desc="Prices"):
            tk, ok = f.result()
            if ok:
                success.add(tk)

    # 4-3 合併並輸出 Parquet
    frames = []
    for tk in success:
        path = PRICES_DIR / f"{tk}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if "Close" in df.columns:
            frames.append(df["Close"].rename(tk))
    if frames:
        pd.concat(frames, axis=1).sort_index().to_parquet(
            PARQUET_PATH, compression="gzip"
        )

    # 4-4 與舊檔比較，避免無意義 commit
    new_df = pd.DataFrame(fundamentals)
    if JSON_PATH.exists():
        old_df = pd.read_json(JSON_PATH, orient="records")
        if old_df.equals(new_df):
            print("ℹ️ 成分股和基本面未變動，跳過提交")
            return
    new_df.to_json(JSON_PATH, orient="records", indent=2)
    print(f"✅ Update finished in {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
