import os, json, time, requests, pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import yfinance as yf

DATA_DIR      = Path("data")
PRICES_DIR    = DATA_DIR / "prices"
PARQUET_PATH  = DATA_DIR / "prices.parquet.gz"
JSON_PATH     = DATA_DIR / "preprocessed_data.json"
MAX_WORKERS   = 20

DATA_DIR.mkdir(exist_ok=True); PRICES_DIR.mkdir(exist_ok=True)

# ---------- A. 指數成分股 ----------
def fetch_sp500_official():
    try:
        html = requests.get("https://www.spglobal.com/spdji/en/indices/equity/sp-500/#overview", timeout=10).text
        s = html.find("indexMembers"); l = html.find("[", s); r = html.find("]", l) + 1
        return [m["symbol"] for m in json.loads(html[l:r])]
    except Exception as e: print("[A] SP500 official failed:", e); return []

def fetch_nasdaq100_official():
    try:
        url = "https://api.nasdaq.com/api/quote/NDX/constituents"
        hdr = {"User-Agent":"Mozilla/5.0"}
        rows = requests.get(url, headers=hdr, timeout=10).json()["data"]["rows"]
        return [row["symbol"] for row in rows]
    except Exception as e: print("[A] NDX official failed:", e); return []

def fetch_index_from_fmp(etf):
    tok = os.getenv("FMP_TOKEN");  # Basic plan: 250 req/day
    if not tok: return []
    try:
        url = f"https://financialmodelingprep.com/api/v3/etf-holder/{etf}?apikey={tok}"
        rows = requests.get(url, timeout=10).json()
        return [r["asset"] for r in rows]
    except Exception as e: print("[B] FMP", etf, "failed:", e); return []

# ---------- B. 備援 ETF / Wiki ----------
def get_etf_holdings(etf):
    try:
        h = yf.Ticker(etf).holdings
        return h['symbol'].tolist() if h is not None else []
    except: return []

def get_sp500_from_wiki():
    try:
        url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        return pd.read_html(url)[0]['Symbol'].str.replace('.', '-').tolist()
    except: return []

def get_nasdaq100_from_wiki():
    try:
        url='https://en.wikipedia.org/wiki/Nasdaq-100'
        return pd.read_html(url)[4]['Ticker'].tolist()
    except: return []

def get_sp500_list():
    for fn in (fetch_sp500_official, lambda: fetch_index_from_fmp("VOO")):
        s = fn();  # official ➜ FMP
        if s: return s
    return get_etf_holdings("VOO") or get_sp500_from_wiki()

def get_nasdaq100_list():
    for fn in (fetch_nasdaq100_official, lambda: fetch_index_from_fmp("QQQ")):
        s = fn()
        if s: return s
    return get_etf_holdings("QQQ") or get_nasdaq100_from_wiki()

# ---------- C. 基本面欄位 ----------
EXTRA = ['priceToBook','priceToSalesTrailing12Months','ebitdaMargins',
         'grossMargins','operatingMargins','debtToEquity']

def fetch_fundamentals(tk):
    try:
        info = yf.Ticker(tk).info
        if not info.get('marketCap'): return None
        d={'ticker':tk,'marketCap':info.get('marketCap'),'sector':info.get('sector'),
           'trailingPE':info.get('trailingPE'),'forwardPE':info.get('forwardPE'),
           'dividendYield':info.get('dividendYield'),'returnOnEquity':info.get('returnOnEquity'),
           'revenueGrowth':info.get('revenueGrowth'),'earningsGrowth':info.get('earningsGrowth')}
        for k in EXTRA: d[k]=info.get(k)
        return d
    except: return None

def fetch_history(tk):
    try:
        df=yf.download(tk,start="1990-01-01",progress=False,auto_adjust=True)['Close']
        if df.empty: return False
        df.to_csv(PRICES_DIR/f"{tk}.csv"); return True
    except: return False

# ---------- D. 主入口 ----------
def main():
    t0=time.time()
    sp=set(get_sp500_list()); nd=set(get_nasdaq100_list())
    tickers=sorted(sp|nd); print("Total symbols:",len(tickers))
    if not tickers: return

    # 基本面
    fun=[]
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        for f in tqdm(as_completed({ex.submit(fetch_fundamentals,t):t for t in tickers}),
                      total=len(tickers),desc="Fundamentals"):
            d=f.result();  d and fun.append(d)
    for d in fun: d['in_sp500']=d['ticker'] in sp; d['in_nasdaq100']=d['ticker'] in nd

    # 價格
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        _=[f.result() for f in tqdm(as_completed({ex.submit(fetch_history,t):t for t in tickers}),
                                    total=len(tickers),desc="Prices")]

    # Parquet
    frames=[pd.read_csv(PRICES_DIR/f"{t}.csv",index_col='Date',parse_dates=True)['Close'].rename(t)
            for t in tickers if (PRICES_DIR/f"{t}.csv").exists()]
    pd.concat(frames,axis=1).sort_index().to_parquet(PARQUET_PATH,compression='gzip')

    # 如內容沒變則跳過 commit
    old_hash = pd.util.hash_pandas_object(pd.read_json(JSON_PATH,orient='records'),index=False).sum() if JSON_PATH.exists() else None
    pd.DataFrame(fun).to_json(JSON_PATH,orient='records',indent=2)
    new_hash = pd.util.hash_pandas_object(pd.read_json(JSON_PATH,orient='records'),index=False).sum()
    print("Done in %.1fs"% (time.time()-t0))
    if old_hash==new_hash: print("No change in fundamentals."); exit(0)

if __name__=="__main__": main()
