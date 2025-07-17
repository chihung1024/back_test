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
def sp500_official():
    try:
        html = requests.get("https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview", timeout=10).text
        i = html.find("indexMembers")
        if i == -1: return []
        l = html.find("[", i); r = html.find("]", l) + 1
        return [m["symbol"] for m in json.loads(html[l:r])]
    except Exception:
        return []

def nasdaq_official():
    try:
        hdr = {"User-Agent": "Mozilla/5.0"}
        rows = requests.get("https://api.nasdaq.com/api/quote/NDX/constituents", headers=hdr, timeout=10).json()["data"]["rows"]
        return [r["symbol"] for r in rows]
    except Exception:
        return []

def fmp_etf_components(etf: str):
    key = os.getenv("FMP_TOKEN")
    if not key:
        return []
    try:
        url  = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={key}"
        rows = requests.get(url, timeout=10).json()
        return [row.get("symbol") or row.get("asset") for row in rows if isinstance(row, dict)]
    except Exception:
        return []

def etf_holdings(etf: str):
    try:
        hold = yf.Ticker(etf).holdings
        return hold["symbol"].tolist() if hold is not None else []
    except Exception:
        return []

def wiki_sp500():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        return pd.read_html(url)[0]["Symbol"].str.replace(".", "-").tolist()
    except Exception:
        return []

def wiki_nasdaq100():
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        return pd.read_html(url)[4]["Ticker"].tolist()
    except Exception:
        return []

def get_sp500():
    for fn in (sp500_official, lambda: fmp_etf_components("VOO")):
        res = fn()
        if res:
            return res
    return etf_holdings("VOO") or wiki_sp500()

def get_nasdaq100():
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

def fetch_history(ticker: str):
    """
    下載成功 → 回傳 (ticker, True)  
    失敗     → 回傳 (ticker, False)
    """
    try:
        df = yf.download(ticker, start="1990-01-01", progress=False, auto_adjust=True)
        if df.empty or "Close" not in df.columns:
            return ticker, False
        df[["Close"]].to_csv(PRICES_DIR / f"{ticker}.csv")
        return ticker, True
    except Exception:
        return ticker, False

# ─── 3. 主流程 ───────────────────────────────────────────────
def main():
    t0 = time.time()
    sp500_tickers   = set(get_sp500())
    nasdaq_tickers  = set(get_nasdaq100())
    all_tickers     = sorted(sp500_tickers | nasdaq_tickers)

    if not all_tickers:
        print("❌ 無法取得任何成分股，結束執行"); return
    print("Total symbols:", len(all_tickers))

    # 基本面
    fundamentals = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        fut_map = {ex.submit(fetch_fundamentals, t): t for t in all_tickers}
        for fut in tqdm(as_completed(fut_map), total=len(fut_map), desc="Fundamentals"):
            data = fut.result()
            if data:
                fundamentals.append(data)

    for row in fundamentals:
        row["in_sp500"]     = row["ticker"] in sp500_tickers
        row["in_nasdaq100"] = row["ticker"] in nasdaq_tickers

    # 歷史價格
    success = set()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        fut_map = {ex.submit(fetch_history, t): t for t in all_tickers}
        for fut in tqdm(as_completed(fut_map), total=len(fut_map), desc="Prices"):
            tkr, ok = fut.result()
            if ok:
                success.add(tkr)

    frames = []
    for tkr in success:
        fp = PRICES_DIR / f"{tkr}.csv"
        if not fp.exists():
            continue
        df = pd.read_csv(fp, index_col="Date", parse_dates=True)
        if "Close" in df.columns:
            frames.append(df["Close"].rename(tkr))

    if frames:
        pd.concat(frames, axis=1).sort_index().to_parquet(PARQUET_FILE, compression="gzip")

    # 基本面變更偵測
    new_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)
    if JSON_FILE.exists():
        old_df = pd.read_json(JSON_FILE, orient="records")
        if new_df.equals(old_df):
            print("ℹ️ 基本面無變動，跳過寫檔"); return

    new_df.to_json(JSON_FILE, orient="records", indent=2)
    print(f"✅ 更新完成，耗時 {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
