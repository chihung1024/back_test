# backtest_route.py: 專門處理與投資組合回測相關的 API 路由

from flask import Blueprint, request, jsonify
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import traceback

# 使用相對路徑從上層的 utils 模組匯入核心邏輯
from ..utils.data_handler import read_price_data_from_repo, validate_data_completeness
from ..utils.simulation import run_simulation
from ..utils.calculations import calculate_metrics

# 建立一個名為 'backtest' 的藍圖
backtest_bp = Blueprint('backtest', __name__)

@backtest_bp.route('/backtest', methods=['POST'])
def backtest_handler():
    """處理投資組合回測請求。"""
    try:
        data = request.get_json()
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        all_tickers = set(ticker for p in data['portfolios'] for ticker in p['tickers'])
        benchmark_ticker = data.get('benchmark')
        if benchmark_ticker:
            all_tickers.add(benchmark_ticker)
            
        all_tickers_tuple = tuple(sorted(list(all_tickers)))
        if not all_tickers_tuple:
            return jsonify({'error': '請至少在一個投資組合中設定一項資產。'}), 400
            
        df_prices_raw = read_price_data_from_repo(all_tickers_tuple, start_date_str, end_date_str)
        
        if df_prices_raw.empty:
            return jsonify({'error': f"在指定的時間範圍內找不到任何請求的股票數據。"}), 400

        # ... (其餘邏輯與之前版本相同) ...
        problematic_tickers_info = validate_data_completeness(df_prices_raw, all_tickers_tuple, pd.to_datetime(start_date_str))
        warning_message = None
        if problematic_tickers_info:
            tickers_str = ", ".join([f"{item['ticker']} (從 {item['start_date']} 開始)" for item in problematic_tickers_info])
            warning_message = f"部分資產的數據起始日晚於您的選擇。回測已自動調整至最早的共同可用日期。週期受影響的資產：{tickers_str}"
            
        df_prices_common = df_prices_raw.dropna()
        if df_prices_common.empty:
            return jsonify({'error': '在指定的時間範圍內，找不到所有股票的共同交易日。'}), 400
            
        initial_amount = float(data['initialAmount'])
        benchmark_result = None
        benchmark_history = None
        
        if benchmark_ticker and benchmark_ticker in df_prices_common.columns:
            benchmark_config = {'name': benchmark_ticker, 'tickers': [benchmark_ticker], 'weights': [100], 'rebalancingPeriod': 'never'}
            benchmark_result = run_simulation(benchmark_config, df_prices_common, initial_amount)
            if benchmark_result:
                benchmark_history = pd.DataFrame(benchmark_result['portfolioHistory']).set_index('date')
                benchmark_history.index = pd.to_datetime(benchmark_history.index)
                
        results = [res for p_config in data['portfolios'] if p_config['tickers'] and (res := run_simulation(p_config, df_prices_common, initial_amount, benchmark_history))]
        
        if not results:
            return jsonify({'error': '沒有足夠的共同交易日來進行回測。'}), 400
            
        if benchmark_result and benchmark_history is not None:
            temp_metrics = calculate_metrics(benchmark_history)
            benchmark_result.update(temp_metrics)
            benchmark_result['beta'] = 1.0
            benchmark_result['alpha'] = 0.00

        return jsonify({'data': results, 'benchmark': benchmark_result, 'warning': warning_message})
        
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': f'伺服器發生未預期的錯誤: {str(e)}'}), 500
