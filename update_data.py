import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from vercel_blob import put
import io
import logging
from api.utils.data_provider import get_sp500_tickers, get_stock_data, get_financials
import time

# --- 設定 ---
# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 從 .env 檔案載入環境變數 (主要為了本地執行)
load_dotenv()

# --- 主要函式 ---
def upload_df_to_blob(df: pd.DataFrame, blob_path: str):
    """將 Pandas DataFrame 以 Parquet 格式上傳到 Vercel Blob"""
    if df.empty:
        logger.warning(f"DataFrame for {blob_path} is empty. Skipping upload.")
        return
    try:
        logger.info(f"Uploading {blob_path} to Vercel Blob...")
        # 將 DataFrame 寫入記憶體中的 Parquet 檔案
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=True)
        parquet_buffer.seek(0)
        
        # 上傳到 Vercel Blob
        # 注意：os.getenv('BLOB_READ_WRITE_TOKEN') 必須要有值
        put(blob_path, parquet_buffer, options={'token': os.getenv('BLOB_READ_WRITE_TOKEN')})
        logger.info(f"Successfully uploaded {blob_path}.")
    except Exception as e:
        logger.error(f"Failed to upload {blob_path}: {e}")

def update_all_data():
    """
    主執行函式：獲取所有數據並上傳到 Blob
    """
    logger.info("--- Starting Data Update Process ---")
    
    # 1. 更新 S&P 500 成分股列表
    tickers = get_sp500_tickers()
    sp500_df = pd.DataFrame(tickers, columns=['Symbol'])
    upload_df_to_blob(sp500_df, 'constituents/sp500.parquet')
    
    # 2. 遍歷所有股票，更新價格、股息和財務數據
    start_date = "2010-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')
    all_financials = []

    for i, ticker in enumerate(tickers):
        logger.info(f"Processing {ticker} ({i+1}/{len(tickers)})...")
        
        # 獲取股價和股息
        price_df, dividends_df, source = get_stock_data(ticker, start_date, end_date)
        
        if not price_df.empty:
            upload_df_to_blob(price_df, f'prices/adjusted/{ticker}.parquet')
        if not dividends_df.empty:
            upload_df_to_blob(dividends_df, f'prices/dividends/{ticker}.parquet')
            
        # 獲取財務數據
        financial_data = get_financials(ticker)
        if financial_data:
            all_financials.append(financial_data)
            
        # 避免過於頻繁地請求 API，加入短暫延遲
        time.sleep(0.5) 

    # 3. 上傳整合後的財務數據
    if all_financials:
        financials_df = pd.DataFrame(all_financials).set_index('symbol')
        upload_df_to_blob(financials_df, 'fundamentals/fundamentals.parquet')

    logger.info("--- Data Update Process Finished ---")

if __name__ == "__main__":
    # 建立一個 .env 檔案在專案根目錄，並填入您的 BLOB_READ_WRITE_TOKEN
    # BLOB_READ_WRITE_TOKEN="vercel_blob_rw_...your_token..."
    if not os.getenv('BLOB_READ_WRITE_TOKEN'):
        logger.error("FATAL: BLOB_READ_WRITE_TOKEN environment variable not set.")
        logger.error("Please create a .env file in the root directory and add your token.")
    else:
        update_all_data()
