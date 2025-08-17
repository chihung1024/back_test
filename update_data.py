# ── update_data.py（優化完整版本）────────────────────────────
# 功能：
# 1. 先抓官方 S&P 500 / Nasdaq-100 成分；失敗改用 FMP；再失敗回 ETF / Wiki
# 2. 多執行緒下載基本面與歷史價格
# 3. 只對下載成功的股票合併 Parquet，避免 “KeyError: 'Close'”
# 4. 若基本面資料與前次相同就跳過寫檔，減少無意義 commit
# 5. 正規化代碼（BRK.B→BRK-B 等）與後端一致

import os, json, time, requests, pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf

# ─── 資料夾設定 ──────────────────────────────────────────────
DATA_DIR = Path("data")
PRICES_DIR = DATA_DIR / "prices"
PARQUET_FILE = DATA_DIR / "prices.parquet.gz"
JSON_FILE = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS = 20

DATA_DIR.mkdir(exist_ok=True)
PRICES_DIR.mkdir(exist_ok=True)

# ─── 代碼正規化（與後端一致，避免 BRK.B / BF.B 等） ─────────
def normalize_ticker_for_yahoo(ticker: str) -> str:
    if not isinstance(ticker, str):
        return ticker
    t = ticker.strip()
    if "." in t:
        parts = t.split(".")
        if len(parts) == 2 and parts[1].isalpha():
            t = parts + "-" + parts[1]
    t = t.replace(" ", "")
    return t

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
        url = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={key}"
        rows = requests.get(url, timeout=10).json()
        return [
            (row.get("symbol") or row.get("asset"))
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
        # 將 . 改成 - 以符合 Yahoo
        return pd.read_html(url)[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
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
    成功：回傳 (ticker, True) 並在 data/prices 生成 .csv.gz（僅 Close）
    失敗：重試後回傳 (ticker, False)

    額外：
    1) 索引命名為 'Date'，利於 read_csv(index_col='Date')
    2) 輸出 gzip 壓縮
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = yf.download(
                ticker,
                start="1990-01-01",
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
        except Exception as e:
            if attempt == max_retries:
                return ticker, False
            time.sleep(pause_sec)

# ─── 3. 主流程 ───────────────────────────────────────────────
def main():
    t0 = time.time()

    # 取得指數成分股
    sp500_list = get_sp500()
    ndx_list = get_nasdaq100()

    # 正規化代碼並去重
    sp500_set = set(normalize_ticker_for_yahoo(t) for t in sp500_list if t)
    ndx_set = set(normalize_ticker_for_yahoo(t) for t in ndx_list if t)
    tickers = sorted(sp500_set | ndx_set)

    if not tickers:
        print("❌ 無法取得任何成份股，結束執行")
        return

    print("Total symbols:", len(tickers))

    # 3-1 基本面
    fundamentals = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc="Fundamentals"):
            data = fut.result()
            if data:
                fundamentals.append(data)

    for row in fundamentals:
        row["in_sp500"] = row["ticker"] in sp500_set
        row["in_nasdaq100"] = row["ticker"] in ndx_set

    # 3-2 歷史價格（逐檔下載）
    success = set()
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        jobs = {ex.submit(fetch_history, t): t for t in tickers}
        for fut in tqdm(as_completed(jobs), total=len(jobs), desc="Prices"):
            tk, ok = fut.result()
            if ok:
                success.add(tk)

    # 3-2-1 合併成功下載的 .csv.gz 為寬表並輸出 Parquet
    frames = []
    missing_files = 0
    for tk in success:
        # 注意：fetch_history 輸出的副檔名為 .csv.gz
        csv_path_gz = PRICES_DIR / f"{tk}.csv.gz"
        if csv_path_gz.exists():
            df = pd.read_csv(csv_path_gz, index_col="Date", parse_dates=True)
            # 僅在確實有 Close 欄位時才納入
            if "Close" in df.columns:
                frames.append(df["Close"].rename(tk))
        else:
            # 相容舊版可能寫成 .csv（不壓縮）
            csv_path = PRICES_DIR / f"{tk}.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path, index_col="Date", parse_dates=True)
                if "Close" in df.columns:
                    frames.append(df["Close"].rename(tk))
            else:
                missing_files += 1

    if frames:
        wide = pd.concat(frames, axis=1).sort_index()
        # 只在非空時寫檔
        if not wide.empty:
            wide.to_parquet(PARQUET_FILE, compression="gzip")
            print(f"✅ 合併價格完成：{len(wide.columns)} 檔，日期範圍 {wide.index.min().date()} ~ {wide.index.max().date()}")
        else:
            print("⚠️ 合併結果為空，跳過寫入 Parquet")
    else:
        print("⚠️ 無任何成功的價格檔可合併，跳過 Parquet")

    if missing_files:
        print(f"ℹ️ 有 {missing_files} 檔下載成功但缺少對應CSV檔（可能是舊檔名或被清理）。")

    # 3-3 基本面變更偵測
    new_df = pd.DataFrame(fundamentals).sort_values("ticker").reset_index(drop=True)
    if JSON_FILE.exists():
        try:
            old_df = pd.read_json(JSON_FILE, orient="records")
            # 欄位對齊避免 equals 誤判
            new_df_aligned = new_df.reindex(columns=sorted(new_df.columns))
            old_df_aligned = old_df.reindex(columns=sorted(old_df.columns))
            if new_df_aligned.equals(old_df_aligned):
                print("ℹ️ 基本面無變動，跳過寫檔")
                elapsed = time.time() - t0
                print(f"⏱️ 總耗時 {elapsed:.1f}s")
                return
        except Exception:
            # 若舊檔壞損則直接覆蓋
            pass

    new_df.to_json(JSON_FILE, orient="records", indent=2)
    print(f"✅ 更新完成，耗時 {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
