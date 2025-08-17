# api/index.py (最終高效能版本)

from flask import Flask, request, jsonify
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os

# 引入您專案中已經存在的數據處理和計算模組
from api.utils import data_handler, calculations 

app = Flask(__name__)

# --- API 端點 ---

@app.route('/api/scan', methods=['POST'])
def scan_handler():
    """
    高效能的掃描 API，直接讀取 GitHub Repo 中預處理好的 Parquet 數據。
    """
    try:
        data = request.get_json()
        tickers = data.get('tickers', [])
        benchmark_ticker = data.get('benchmark')
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime('%Y-%m-%d')

        if not tickers:
            return jsonify({'error': '股票代碼列表不可為空。'}), 400

        all_tickers_to_fetch = set(tickers)
        if benchmark_ticker:
            all_tickers_to_fetch.add(benchmark_ticker)
        
        # 從 data_handler 讀取預處理數據
        df_prices_raw = data_handler.read_price_data_from_repo(
            tuple(sorted(list(all_tickers_to_fetch))),
            start_date_str,
            end_date_str
        )

        if df_prices_raw.empty:
            return jsonify([{'ticker': t, 'error': '在指定範圍找不到數據'} for t in tickers])

        benchmark_history = None
        if benchmark_ticker and benchmark_ticker in df_prices_raw.columns:
            benchmark_prices = df_prices_raw[[benchmark_ticker]].dropna()
            if not benchmark_prices.empty:
                benchmark_history = benchmark_prices.rename(columns={benchmark_ticker: 'value'})

        results = []
        requested_start_date = pd.to_datetime(start_date_str)
        
        for ticker in tickers:
            try:
                if ticker not in df_prices_raw.columns or df_prices_raw[ticker].isnull().all():
                    results.append({'ticker': ticker, 'error': '找不到數據'})
                    continue

                stock_prices = df_prices_raw[ticker].dropna()
                if stock_prices.empty:
                    results.append({'ticker': ticker, 'error': '範圍內無數據'})
                    continue

                problematic_info = data_handler.validate_data_completeness(
                    df_prices_raw, [ticker], requested_start_date
                )
                note = f"(從 {problematic_info[0]['start_date']} 開始)" if problematic_info else None
                
                history_df = stock_prices.to_frame(name='value')
                metrics = calculations.calculate_metrics(history_df, benchmark_history)
                results.append({'ticker': ticker, **metrics, 'note': note})

            except Exception as e:
                print(f"處理 {ticker} 時發生錯誤: {e}") 
                results.append({'ticker': ticker, 'error': '計算錯誤'})
                
        return jsonify(results)
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({'error': f'伺服器發生未預期的錯誤: {str(e)}'}), 500

# 為了簡潔，暫時移除了 backtest 和 screener 的後端邏輯
# 您可以後續參照 scan_handler 的模式，將它們也改為讀取預載數據

@app.route('/', methods=['GET'])
def index():
    return "Python backend is running."
