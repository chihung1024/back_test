import pandas as pd
import yfinance as yf
from stooqpy import Stooq
import logging
from datetime import datetime, timedelta

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 成分股獲取 ---
def get_sp500_tickers():
    """
    從維基百科獲取最新的 S&P 500 成分股列表，並提供備援。
    返回:
        list: 股票代碼列表。
    """
    try:
        logger.info("Attempting to fetch S&P 500 tickers from Wikipedia...")
        url = '''https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'''
        tables = pd.read_html(url)
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].tolist()
        # Yahoo Finance uses '-' for dots in tickers (e.g., BRK.B -> BRK-B)
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        logger.info(f"Successfully fetched {len(tickers)} tickers from Wikipedia.")
        return tickers
    except Exception as e:
        logger.error(f"Failed to fetch tickers from Wikipedia: {e}. Using fallback list.")
        # 提供一個靜態的備援列表，以防 Wikipedia 爬取失敗
        return [
            'AAPL', 'MSFT', 'AMZN', 'NVDA', 'GOOGL', 'GOOG', 'TSLA', 'META',
            'BRK-B', 'UNH', 'JPM', 'JNJ', 'V', 'PG', 'XOM', 'HD', 'CVX', 'MA',
            'ABBV', 'LLY', 'PEP', 'COST', 'AVGO', 'MRK', 'BAC', 'KO'
            # ...可以根據需要增加更多股票作為備援
        ]

# --- 股價與財務數據獲取 ---
def get_stock_data(ticker: str, start_date: str, end_date: str):
    """
    獲取單一股票的數據，包含價格、股息、分割，並提供備援。
    優先使用 yfinance，失敗則嘗試 stooq。
    返回:
        tuple: (price_data, dividend_data, source)
               price_data: 包含 OHLCV 的 DataFrame
               dividend_data: 包含股息的 DataFrame
               source: 'yfinance' 或 'stooq'
    """
    try:
        logger.info(f"Fetching data for {ticker} from yfinance...")
        stock = yf.Ticker(ticker)
        
        # 獲取已還原的價格數據
        price_df = stock.history(start=start_date, end=end_date, auto_adjust=True)
        if price_df.empty:
            raise ValueError("yfinance returned empty price dataframe.")
            
        # 獲取股息數據
        dividends_df = stock.dividends.to_frame()
        dividends_df = dividends_df[dividends_df.index >= pd.to_datetime(start_date)]
        
        logger.info(f"Successfully fetched data for {ticker} from yfinance.")
        return price_df, dividends_df, 'yfinance'
        
    except Exception as e_yf:
        logger.warning(f"yfinance failed for {ticker}: {e_yf}. Trying stooq as fallback.")
        try:
            stq = Stooq()
            # Stooq 的日期格式是 YYYYMMDD
            price_df_stooq = stq.get_data(ticker=ticker, start=pd.to_datetime(start_date).strftime('%Y%m%d'), end=pd.to_datetime(end_date).strftime('%Y%m%d'))
            if price_df_stooq.empty:
                raise ValueError("stooq returned empty dataframe.")
            
            # Stooq 提供的是未還原數據，且欄位名稱不同，需要進行轉換
            price_df_stooq.rename(columns={'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume'}, inplace=True)
            
            logger.warning(f"Successfully fetched data for {ticker} from stooq. NOTE: Data is NOT adjusted for splits/dividends.")
            # Stooq 不提供股息數據，返回一個空的 DataFrame
            return price_df_stooq, pd.DataFrame(), 'stooq'
            
        except Exception as e_stooq:
            logger.error(f"All data sources failed for {ticker}. yfinance: {e_yf}, stooq: {e_stooq}")
            return pd.DataFrame(), pd.DataFrame(), 'failed'

def get_financials(ticker: str):
    """
    獲取單一股票的關鍵財務數據。
    """
    try:
        logger.info(f"Fetching financial info for {ticker}...")
        stock_info = yf.Ticker(ticker).info
        
        # 提取我們需要的關鍵指標，並提供預設值以防萬一
        financial_data = {
            'symbol': ticker,
            'marketCap': stock_info.get('marketCap', 0),
            'trailingPE': stock_info.get('trailingPE'),
            'forwardPE': stock_info.get('forwardPE'),
            'priceToBook': stock_info.get('priceToBook'),
            'enterpriseToRevenue': stock_info.get('enterpriseToRevenue'),
            'enterpriseToEbitda': stock_info.get('enterpriseToEbitda'),
            'profitMargins': stock_info.get('profitMargins', 0),
            'totalRevenue': stock_info.get('totalRevenue', 0),
            'grossProfits': stock_info.get('grossProfits', 0),
            'ebitda': stock_info.get('ebitda', 0),
            'shortName': stock_info.get('shortName', 'N/A')
        }
        return financial_data
    except Exception as e:
        logger.error(f"Failed to get financial info for {ticker}: {e}")
        return None
