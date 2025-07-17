import os
import pandas as pd
from pandas.tseries.offsets import BDay
from cachetools import cached, TTLCache
import json
from pathlib import Path

# --- 快取設定 ---
# 快取現在用於緩存從網路 URL 讀取的 DataFrame，避免重複下載
cache = TTLCache(maxsize=256, ttl=1800) # 快取 30 分鐘

# --- 資料路徑設定 ---
# 預處理的 JSON 檔案仍然從本地讀取，因為它很小
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FOLDER = BASE_DIR / "data"
PREPROCESSED_JSON_PATH = DATA_FOLDER / "preprocessed_data.json"

@cached(cache)
def read_price_data_from_repo(tickers: tuple, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """
    從遠端 GitHub data 分支的 raw URL 讀取 CSV 檔案。
    """
    # 從 Vercel 的環境變數中動態獲取倉庫擁有者和名稱
    # 如果在本地開發，則使用預設值（請根據您的情況修改）
    owner = os.environ.get('VERCEL_GIT_REPO_OWNER', 'chihung1024') 
    repo = os.environ.get('VERCEL_GIT_REPO_SLUG', 'back_test')
    
    # 建立基礎 URL
    base_url = f"https://raw.githubusercontent.com/{owner}/{repo}/data/prices"
    
    all_prices = []
    for ticker in tickers:
        file_url = f"{base_url}/{ticker}.csv"
        try:
            # Pandas 可以直接從 URL 讀取 CSV
            df = pd.read_csv(file_url, index_col='Date', parse_dates=True)
            df.rename(columns={'Close': ticker}, inplace=True)
            all_prices.append(df)
        except Exception as e:
            # 如果某個檔案不存在或讀取失敗，則在後端日誌中印出警告
            print(f"警告：無法從 URL 讀取股票 {ticker} 的價格檔案: {e}")

    if not all_prices:
        return pd.DataFrame()

    combined_df = pd.concat(all_prices, axis=1)
    mask = (combined_df.index >= start_date_str) & (combined_df.index <= end_date_str)
    return combined_df.loc[mask]


@cached(cache)
def get_preprocessed_data():
    """
    從遠端 GitHub data 分支的 raw URL 讀取預處理好的 JSON 數據。
    """
    owner = os.environ.get('VERCEL_GIT_REPO_OWNER', 'chihung1024') 
    repo = os.environ.get('VERCEL_GIT_REPO_SLUG', 'back_test')
    url = f"https://raw.githubusercontent.com/{owner}/{repo}/data/preprocessed_data.json"
    
    try:
        # 使用 pandas 讀取 json url
        return pd.read_json(url).to_dict('records')
    except Exception as e:
        print(f"錯誤：無法從 URL 讀取 preprocessed_data.json: {e}")
        # 在出錯時回傳空列表，避免應用程式崩潰
        return []


def validate_data_completeness(df_prices_raw, all_tickers, requested_start_date):
    """
    檢查是否有任何股票的數據起始日顯著晚於請求的起始日。
    """
    problematic_tickers = []
    for ticker in all_tickers:
        if ticker in df_prices_raw.columns:
            first_valid_date = df_prices_raw[ticker].first_valid_index()
            if first_valid_date is not None and first_valid_date > requested_start_date + BDay(5):
                problematic_tickers.append({'ticker': ticker, 'start_date': first_valid_date.strftime('%Y-%m-%d')})
    return problematic_tickers
