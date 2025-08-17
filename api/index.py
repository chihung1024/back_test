# api/index.py (修改後)

from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay, MonthEnd
import sys
import os
from io import StringIO
from cachetools import cached, TTLCache
import requests

# 【第 1 步：引入必要的模組】
# 引入您專案中已經存在的數據處理和計算模組
from api.utils import data_handler, calculations 

app = Flask(__name__)

# --- 快取設定 (保留) ---
cache = TTLCache(maxsize=128, ttl=600)

# --- 移除不再需要的 Gist URL ---
# GIST_RAW_URL = os.environ.get('GIST_RAW_URL')

# --- 移除不再需要的重複函式 ---
# calculate_metrics 和 run_simulation 函式現在將從 api/utils/ 中呼叫，
# download_data_silently 則完全不再需要。

# --- API 端點 ---
@app.route('/api/backtest', methods=['POST'])
def backtest_handler():
    # ... 您的 backtest_handler 邏輯保持不變 ...
    # 為了保持專注，我們暫時只修改 scan_handler
    # 建議後續也用同樣的邏輯優化 backtest_handler
    try:
        data = request.get_json()
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime('%Y-%m-%d')
        all_tickers = set(ticker for p in data['portfolios'] for ticker in p['tickers'])
        benchmark_ticker = data.get('benchmark')
        if benchmark_ticker: all_tickers.add(benchmark_ticker)
        all_tickers_tuple = tuple(sorted(list(all_tickers)))
        if not all_tickers_tuple: return jsonify({'error': '請至少在一個投資組合中設定一項資產。'}), 400
        
        # 這裡仍然是即時下載，未來可以優化
        old_stdout = sys.stdout; sys.stdout = StringIO()
        try:
            df_prices_raw = yf.download(list(all_tickers_tuple), start=start_date_str, end=end_date_str, auto_adjust=True, progress=False)['Close']
        finally:
            sys.stdout = old_stdout
            
        if isinstance(df_prices_raw, pd.Series): df_prices_raw = df_prices_raw.to_frame(name=all_tickers_tuple[0])
        if df_prices_raw.isnull().all().any():
            failed_tickers = df_prices_raw.columns[df_prices_raw.isnull().all()].tolist()
            return jsonify({'error': f"無法獲取以下股票代碼的數據: {', '.join(failed_tickers)}"}), 400
        problematic_tickers_info = validate_data_completeness(df_prices_raw, all_tickers_tuple, pd.to_datetime(start_date_str))
        warning_message = None
        if problematic_tickers_info:
            tickers_str = ", ".join([f"{item['ticker']} (從 {item['start_date']} 開始)" for item in problematic_tickers_info])
            warning_message = f"部分資產的數據起始日晚於您的選擇。回測已自動調整至最早的共同可用日期。週期受影響的資產：{tickers_str}"
        df_prices_common = df_prices_raw.dropna()
        if df_prices_common.empty: return jsonify({'error': '在指定的時間範圍內，找不到所有股票的共同交易日。'}), 400
        initial_amount = float(data['initialAmount'])
        benchmark_result = None; benchmark_history = None
        if benchmark_ticker and benchmark_ticker in df_prices_common.columns:
            benchmark_config = {'name': benchmark_ticker, 'tickers': [benchmark_ticker], 'weights': [100], 'rebalancingPeriod': 'never'}
            benchmark_result = run_simulation(benchmark_config, df_prices_common, initial_amount)
            if benchmark_result:
                benchmark_history = pd.DataFrame(benchmark_result['portfolioHistory']).set_index('date'); benchmark_history.index = pd.to_datetime(benchmark_history.index)
        results = [res for p_config in data['portfolios'] if p_config['tickers'] and (res := run_simulation(p_config, df_prices_common, initial_amount, benchmark_history))]
        if not results: return jsonify({'error': '沒有足夠的共同交易日來進行回測。'}), 400
        if benchmark_result:
            benchmark_result['beta'] = 1.0; benchmark_result['alpha'] = 0.00
            temp_metrics = calculate_metrics(benchmark_history); benchmark_result.update(temp_metrics)
        return jsonify({'data': results, 'benchmark': benchmark_result, 'warning': warning_message})
    except Exception as e:
        import traceback; print(traceback.format_exc())
        return jsonify({'error': f'伺服器發生未預期的錯誤: {str(e)}'}), 500


# 【第 2 步：替換整個 scan_handler 函式】
@app.route('/api/scan', methods=['POST'])
def scan_handler():
    try:
        data = request.get_json()
        tickers = data['tickers']
        benchmark_ticker = data.get('benchmark')
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime('%Y-%m-%d')

        if not tickers:
            return jsonify({'error': '股票代碼列表不可為空。'}), 400

        all_tickers_to_fetch = set(tickers)
        if benchmark_ticker:
            all_tickers_to_fetch.add(benchmark_ticker)
        
        # 【核心改動】不再即時下載，而是從 Repo 讀取預處理好的 Parquet 數據
        df_prices_raw = data_handler.read_price_data_from_repo(
            tuple(sorted(list(all_tickers_to_fetch))),
            start_date_str,
            end_date_str
        )

        if df_prices_raw.empty:
            return jsonify([{'ticker': t, 'error': '找不到數據'} for t in tickers])

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
                    results.append({'ticker': ticker, 'error': '指定範圍內無數據'})
                    continue

                problematic_info = data_handler.validate_data_completeness(
                    df_prices_raw, [ticker], requested_start_date
                )
                note = f"(從 {problematic_info[0]['start_date']} 開始)" if problematic_info else None
                
                history_df = stock_prices.to_frame(name='value')
                
                # 【統一邏輯】呼叫 utils 中的 calculations 模組
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


@app.route('/api/screener', methods=['POST'])
def screener_handler():
    # ... 您的 screener_handler 邏輯保持不變 ...
    try:
        data = request.get_json()
        index = data.get('index', 'sp500')
        min_market_cap = data.get('minMarketCap', 0)
        sector = data.get('sector', 'any')

        # 【統一邏輯】改為呼叫 data_handler
        all_stocks = data_handler.get_preprocessed_data()

        if index == 'sp500':
            base_pool = [s for s in all_stocks if s.get('in_sp500')]
        elif index == 'nasdaq100':
            base_pool = [s for s in all_stocks if s.get('in_nasdaq100')]
        # 未來若增加 Russell 3000，可在此擴充
        # elif index == 'russell3000':
        #     base_pool = [s for s in all_stocks if s.get('in_russell3000')]
        else:
            base_pool = all_stocks

        filtered_stocks = []
        for stock in base_pool:
            if stock.get('marketCap', 0) < min_market_cap:
                continue
            if sector != 'any' and stock.get('sector') != sector:
                continue
            
            filtered_stocks.append(stock['ticker'])

        return jsonify(filtered_stocks)
    except Exception as e:
        import traceback; print(traceback.format_exc())
        return jsonify({'error': f'篩選器發生錯誤: {str(e)}'}), 500


@app.route('/', methods=['GET'])
def index():
    return "Python backend is running."

# --- 保留您原始版本中的輔助函式，因為 backtest_handler 仍然需要它們 ---
def get_rebalancing_dates(df_prices, period):
    if period == 'never': return []
    df = df_prices.copy(); df['year'] = df.index.year; df['month'] = df.index.month
    if period == 'annually': rebalance_dates = df.drop_duplicates(subset=['year'], keep='first').index
    elif period == 'quarterly': df['quarter'] = df.index.quarter; rebalance_dates = df.drop_duplicates(subset=['year', 'quarter'], keep='first').index
    elif period == 'monthly': rebalance_dates = df.drop_duplicates(subset=['year', 'month'], keep='first').index
    else: return []
    return rebalance_dates[1:] if len(rebalance_dates) > 1 else []

def validate_data_completeness(df_prices_raw, all_tickers, requested_start_date):
    problematic_tickers = []
    for ticker in all_tickers:
        if ticker in df_prices_raw.columns:
            first_valid_date = df_prices_raw[ticker].first_valid_index()
            if first_valid_date is not None and first_valid_date > requested_start_date + BDay(5):
                problematic_tickers.append({'ticker': ticker, 'start_date': first_valid_date.strftime('%Y-%m-%d')})
    return problematic_tickers

def calculate_metrics(portfolio_history, benchmark_history=None, risk_free_rate=0):
    # ... (此處保留您原始的 calculate_metrics 內容)
    if portfolio_history.empty or len(portfolio_history) < 2: return {'cagr': 0, 'mdd': 0, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}
    end_value = portfolio_history['value'].iloc[-1]
    start_value = portfolio_history['value'].iloc[0]
    if start_value < 1e-9: return {'cagr': 0, 'mdd': -1, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}
    start_date = portfolio_history.index[0]
    end_date = portfolio_history.index[-1]
    years = (end_date - start_date).days / 365.25
    cagr = (end_value / start_value) ** (1 / years) - 1 if years > 0 else 0
    portfolio_history['peak'] = portfolio_history['value'].cummax()
    portfolio_history['drawdown'] = (portfolio_history['value'] - portfolio_history['peak']) / (portfolio_history['peak'] + 1e-9)
    mdd = portfolio_history['drawdown'].min()
    daily_returns = portfolio_history['value'].pct_change().dropna()
    if len(daily_returns) < 2: return {'cagr': cagr, 'mdd': mdd, 'volatility': 0, 'sharpe_ratio': 0, 'sortino_ratio': 0, 'beta': None, 'alpha': None}
    annual_std = daily_returns.std() * np.sqrt(252)
    annualized_excess_return = cagr - risk_free_rate
    sharpe_ratio = annualized_excess_return / (annual_std + 1e-9)
    daily_risk_free_rate = (1 + risk_free_rate)**(1/252) - 1
    downside_returns = daily_returns - daily_risk_free_rate
    downside_returns[downside_returns > 0] = 0
    downside_std = np.sqrt((downside_returns**2).mean()) * np.sqrt(252)
    sortino_ratio = 0.0
    if downside_std > 1e-9: sortino_ratio = annualized_excess_return / downside_std
    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        benchmark_returns = benchmark_history['value'].pct_change().dropna()
        aligned_returns = pd.concat([daily_returns, benchmark_returns], axis=1, join='inner')
        aligned_returns.columns = ['portfolio', 'benchmark']
        if len(aligned_returns) > 1:
            covariance_matrix = aligned_returns.cov()
            covariance = covariance_matrix.iloc[0, 1]
            benchmark_variance = covariance_matrix.iloc[1, 1]
            if benchmark_variance > 1e-9:
                beta = covariance / benchmark_variance
                bench_end_value = benchmark_history['value'].iloc[-1]
                bench_start_value = benchmark_history['value'].iloc[0]
                bench_cagr = (bench_end_value / bench_start_value) ** (1 / years) - 1 if years > 0 else 0
                expected_return = risk_free_rate + beta * (bench_cagr - risk_free_rate)
                alpha = cagr - expected_return
    if not np.isfinite(sharpe_ratio) or np.isnan(sharpe_ratio): sharpe_ratio = 0.0
    if not np.isfinite(sortino_ratio) or np.isnan(sortino_ratio): sortino_ratio = 0.0
    if beta is not None and (not np.isfinite(beta) or np.isnan(beta)): beta = None
    if alpha is not None and (not np.isfinite(alpha) or np.isnan(alpha)): alpha = None
    return {'cagr': cagr, 'mdd': mdd, 'volatility': annual_std, 'sharpe_ratio': sharpe_ratio, 'sortino_ratio': sortino_ratio, 'beta': beta, 'alpha': alpha}

def run_simulation(portfolio_config, price_data, initial_amount, benchmark_history=None):
    # ... (此處保留您原始的 run_simulation 內容)
    tickers = portfolio_config['tickers']
    weights = np.array(portfolio_config['weights']) / 100.0
    rebalancing_period = portfolio_config['rebalancingPeriod']
    df_prices = price_data[tickers].copy()
    if df_prices.empty: return None
    portfolio_history = pd.Series(index=df_prices.index, dtype=float, name="value")
    rebalancing_dates = get_rebalancing_dates(df_prices, rebalancing_period)
    current_date = df_prices.index[0]
    initial_prices = df_prices.loc[current_date]
    shares = (initial_amount * weights) / (initial_prices + 1e-9)
    portfolio_history.loc[current_date] = initial_amount
    for i in range(1, len(df_prices)):
        current_date = df_prices.index[i]
        current_prices = df_prices.loc[current_date]
        current_value = (shares * current_prices).sum()
        portfolio_history.loc[current_date] = current_value
        if current_date in rebalancing_dates:
            shares = (current_value * weights) / (current_prices + 1e-9)
    portfolio_history.dropna(inplace=True)
    metrics = calculate_metrics(portfolio_history.to_frame('value'), benchmark_history)
    return {'name': portfolio_config['name'], **metrics, 'portfolioHistory': [{'date': date.strftime('%Y-%m-%d'), 'value': value} for date, value in portfolio_history.items()]}
