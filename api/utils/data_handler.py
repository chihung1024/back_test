import os, pandas as pd, requests
from pathlib import Path
from cachetools import cached, TTLCache
from pandas.tseries.offsets import BDay

CACHE = TTLCache(maxsize=256, ttl=43200)   # 12 h
OWNER = os.environ.get('VERCEL_GIT_REPO_OWNER', 'chihung1024')
REPO  = os.environ.get('VERCEL_GIT_REPO_SLUG', 'back_test')
BASE  = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/data"

@cached(CACHE)
@cached(CACHE)
def _read_parquet():
    """
    嘗試讀 Parquet；若伺服器未安裝 pyarrow / fastparquet
    或讀檔失敗，就回傳 None 讓後續流程自動改讀 CSV。
    """
    try:
        import pyarrow  # 若沒裝會觸發例外
    except ModuleNotFoundError:
        return None

    try:
        return pd.read_parquet(f"{BASE}/prices.parquet.gz")
    except Exception:
        return None


@cached(CACHE)
def read_price_data_from_repo(tickers:tuple,start:str,end:str):
    df=_read_parquet()
    if df is not None:
        out=df.loc[start:end,list(tickers)].copy()
        return out.dropna(axis=1,how='all')
    # 回退逐檔 CSV
    frames=[]
    for t in tickers:
        try:
            tmp=pd.read_csv(f"{BASE}/prices/{t}.csv",index_col='Date',parse_dates=True)['Close'].rename(t)
            frames.append(tmp)
        except: pass
    if not frames: return pd.DataFrame()
    comb=pd.concat(frames,axis=1)
    m=(comb.index>=start)&(comb.index<=end)
    return comb.loc[m]

@cached(CACHE)
def get_preprocessed_data():
    try: return requests.get(f"{BASE}/preprocessed_data.json",timeout=10).json()
    except: return []

def validate_data_completeness(df_raw,tickers,req_start):
    prob=[]
    for t in tickers:
        if t in df_raw.columns:
            first=df_raw[t].first_valid_index()
            if first is not None and first>req_start+BDay(5):
                prob.append({'ticker':t,'start_date':first.strftime('%Y-%m-%d')})
    return prob
