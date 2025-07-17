# scan_route.py: 專門處理與個股掃描、篩選器相關的 API 路由

from flask import Blueprint, request, jsonify
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import traceback

# 使用相對路徑從上層的 utils 模組匯入核心邏輯
from ..utils.data_handler import read_price_data_from_repo, get_preprocessed_data, validate_data_completeness
from ..utils.calculations import calculate_metrics

# 建立一個名為 'scan' 的藍圖
scan_bp = Blueprint('scan', __name__)

@scan_bp.route('/scan', methods=['POST'])
def scan_handler():
    """處理個股掃描請求。"""
    try:
        data = request.get_json()
        tickers = data['tickers']
        benchmark_ticker = data.get('benchmark')
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        if not tickers:
            return jsonify({'error': '股票代碼列表不可為空。'}), 400
            
        all_known_tickers = {stock['ticker'] for stock in get_preprocessed_data()}

        all_tickers_to_read = set(tickers)
        if benchmark_ticker:
            all_tickers_to_read.add(benchmark_ticker)
            
        all_tickers_tuple = tuple(sorted(list(all_tickers_to_read)))
        df_prices_raw = read_price_data_from_repo(all_tickers_tuple, start_date_str, end_date_str)
        
        benchmark_history = None
        if benchmark_ticker and benchmark_ticker in df_prices_raw.columns:
            benchmark_prices = df_prices_raw[[benchmark_ticker]].dropna()
            if not benchmark_prices.empty:
                benchmark_history = benchmark_prices.rename(columns={benchmark_ticker: 'value'})
                
        results = []
        requested_start_date = pd.to_datetime(start_date_str)
        
        for ticker in tickers:
            try:
                if ticker not in all_known_tickers:
                    results.append({'ticker': ticker, 'error': '無此代碼'})
                    continue

                if ticker not in df_prices_raw.columns or df_prices_raw[ticker].dropna().empty:
                    results.append({'ticker': ticker, 'error': '指定範圍內無數據'})
                    continue
                
                stock_prices = df_prices_raw[ticker].dropna()
                                    
                note = None
                problematic_info = validate_data_completeness(df_prices_raw[[ticker]], [ticker], requested_start_date)
                if problematic_info:
                    note = f"(從 {problematic_info[0]['start_date']} 開始)"
                    
                history_df = stock_prices.to_frame(name='value')
                metrics = calculate_metrics(history_df, benchmark_history)
                results.append({'ticker': ticker, **metrics, 'note': note})
                
            except Exception as e:
                print(f"處理 {ticker} 時發生錯誤: {e}")
                results.append({'ticker': ticker, 'error': '計算錯誤'})
                
        return jsonify(results)
        
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': f'伺服器發生未預期的錯誤: {str(e)}'}), 500

@scan_bp.route('/screener', methods=['POST'])
def screener_handler():
    """處理股票篩選請求。"""
    try:
        data = request.get_json()
        index = data.get('index', 'sp500')
        filters = data.get('filters', {})
        sector = data.get('sector', 'any')

        all_stocks = get_preprocessed_data()

        if index == 'sp500':
            base_pool = [s for s in all_stocks if s.get('in_sp500')]
        elif index == 'nasdaq100':
            base_pool = [s for s in all_stocks if s.get('in_nasdaq100')]
        # (已修改) 移除 'russell1000' 的判斷，並將 S&P 500 作為預設選項
        else:
            base_pool = [s for s in all_stocks if s.get('in_sp500')]

        filtered_stocks = []
        for stock in base_pool:
            if sector != 'any' and stock.get('sector') != sector:
                continue
            
            match = True
            for key, limits in filters.items():
                stock_value = stock.get(key)
                if stock_value is None or not isinstance(stock_value, (int, float)):
                    match = False
                    break
                
                if limits.get('min') is not None and stock_value < limits['min']:
                    match = False
                    break
                
                if limits.get('max') is not None and stock_value > limits['max']:
                    match = False
                    break
            
            if match:
                filtered_stocks.append(stock['ticker'])

        return jsonify(filtered_stocks)
    except ValueError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': f'篩選器發生錯誤: {str(e)}'}), 500

@scan_bp.route('/all-tickers', methods=['GET'])
def get_all_tickers_handler():
    """提供所有可用於篩選和建議的股票代碼列表。"""
    try:
        all_stocks = get_preprocessed_data()
        ticker_list = [stock['ticker'] for stock in all_stocks if 'ticker' in stock]
        return jsonify(ticker_list)
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': f'無法獲取股票列表: {str(e)}'}), 500
