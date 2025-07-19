# ── update_data.py（增量更新版）─────────────────────────────
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
        if i == -1: return []
        l = html.find("[", i)
        r = html.find("]", l) + 1
        return [m["symbol"] for m in json.loads(html[l:r])]
    except Exception:
        return []

def nasdaq_official() -> list[str]:
    """使用 Nasdaq 官方 JSON API。"""
    try:
        hdr  = {"User-Agent": "Mozilla/5.0"}
        rows = requests.get(
            "https://api.nasdaq.com/api/quote/NDX/constituents",
            headers=hdr, timeout=10
        ).json()["data"]["rows"]
        return [r["symbol"] for r in rows]
    except Exception:
        return []

def fmp_etf_components(etf: str) -> list[str]:
    """以 FMP API 當備援來源。"""
    key = os.getenv("FMP_TOKEN")
    if not key: return []
    try:
        url  = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={key}"
        rows = requests.get(url, timeout=10).json()
        return [
            row.get("symbol") or row.get("asset")
            for row in rows if isinstance(row, dict)
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
        if res: return res
    return etf_holdings("VOO") or wiki_sp500()

def get_nasdaq100() -> list[str]:
    for fn in (nasdaq_official, lambda: fmp_etf_components("QQQ")):
        res = fn()
        if res: return res
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
            "marketCap":        info.get("marketCap"),
            "sector":           info.get("sector"),
            "trailingPE":       info.get("trailingPE"),
            "forwardPE":        info.get("forwardPE"),
            "dividendYield":    info.get("dividendYield"),
            "returnOnEquity":   info.get("returnOnEquity"),
            "revenueGrowth":    info.get("revenueGrowth"),
            "earningsGrowth":   info.get("earningsGrowth"),
        }
        for k in BASIC_EXTRA:
            row[k] = info.get(k)
        return row
    except Exception:
        return None

def fetch_history(
    ticker: str,
    start_date: str,
    max_retries: int = 3,
    pause_sec: float = 1.0
):
    """
    下載單檔歷史價格（從 start_date 起）。
    成功：回傳 (ticker, True) 且在 data/prices 生成 .csv.gz
    失敗：重試後回傳 (ticker, False)
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = yf.download(
                ticker,
                start=start_date,          # ← 依增量邏輯傳入
                progress=False,
                auto_adjust=True
            )
            if df.empty or "Close" not in df.columns:
                raise ValueError("empty frame or no Close column")
            out = df[["Close"]].copy()
            out.index.name = "Date"
            out.to_csv(
                PRICES_DIR / f"{ticker}.csv.gz",
                index_label="Date",
                compression="gzip"
            )
            return ticker, True
        except Exception:
            if attempt == max_retries:
                return ticker, False
            time.sleep(pause_sec)

# ─── 3. 主流程 ───────────────────────────────────────────────
def main():
    t0 = time.time()

    # 3-0 計算增量起始日
    existing_df = None
    start_date  = "1990-01-01"               # 預設全量
    if PARQUET_FILE.exists():
        print(f"ℹ️ 發現現有價格檔：{PARQUET_FILE}")
        existing_df = pd.read_parquet(PARQUET_FILE)
        if not existing_df.empty:
            last_date  = existing_df.index.max()
            start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"📅 只下載 {start_date} 之後的新資料")

    # 3-1 取得成份股
    sp500_set = set(get_sp500())
    ndx_set   = set(get_nasdaq100())
    tickers   = sorted(sp500_set | ndx_set)
    if not tickers:
        print("❌ 無法取得任何成份股，結束執行"); return
    print("Total symbols:", len(tickers))

    # 3-2 基本面
    fundamentals = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc="Fundamentals"):
            data = fut.result()
            if data: fundamentals.append(data)
    for row in fundamentals:
        row["in_sp500"]   = row["ticker"] in sp500_set
        row["in_nasdaq100"] = row["ticker"] in ndx_set

    # 3-3 歷史價格（增量）
    success = set()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_history, t, start_date): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc=f"Prices ≥ {start_date}"):
            tk, ok = fut.result()
            if ok: success.add(tk)

    # 3-4 合併新舊價格
    new_frames = []
    for tk in success:
        csv_path = PRICES_DIR / f"{tk}.csv.gz"
        if csv_path.exists():
            df_new_single = pd.read_csv(csv_path, index_col="Date", parse_dates=True)
            if "Close" in df_new_single.columns:
                new_frames.append(df_new_single["Close"].rename(tk))

    if not new_frames:
        print("ℹ️ 沒有下載到任何新價格資料，跳過 Parquet 更新")
    else:
        new_df = pd.concat(new_frames, axis=1).sort_index()
        if existing_df is not None:
            combined = pd.concat([existing_df, new_df])
            final_df = combined[~combined.index.duplicated(keep="last")]
        else:
            final_df = new_df
        final_df.to_parquet(PARQUET_FILE, compression="gzip")
        print(f"💾 已寫入合併後 Parquet，筆數：{len(final_df)}")

    # 3-5 基本面變更偵測
    new_fund_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)
    if JSON_FILE.exists():
        old_df = pd.read_json(JSON_FILE, orient="records")
        if new_fund_df.equals(old_df):
            print("ℹ️ 基本面無變動，跳過寫檔")
            print(f"✅ 更新完成，耗時 {time.time() - t0:.1f}s"); return
    new_fund_df.to_json(JSON_FILE, orient="records", indent=2)
    print(f"✅ 更新完成，耗時 {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
