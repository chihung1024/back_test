from flask import Flask, request, jsonify
import yfinance as yf, pandas as pd, numpy as np, os, sys, requests
from io import StringIO
from pandas.tseries.offsets import BDay, MonthEnd
from cachetools import cached, TTLCache
from api.utils.calculations import calculate_metrics, EPSILON, RISK_FREE_RATE, DAYS_PER_YEAR, TRADING_DAYS_PER_YEAR

app   = Flask(__name__)
cache = TTLCache(maxsize=128, ttl=600)

# -------------------------------------------------
# 工具：靜默下載（支援快取）
# -------------------------------------------------
@cached(cache)
def download_data_silently(tickers, start_date, end_date):
    old_stdout = sys.stdout
    sys.stdout  = StringIO()           # 關閉 yf.download 的進度列
    try:
        chunks = [tickers[i:i + 15] for i in range(0, len(tickers), 15)]
        parts  = [yf.download(chunk, start=start_date, end=end_date,
                              auto_adjust=True, progress=False)['Close']
                  for chunk in chunks]
        return pd.concat(parts, axis=1)
    finally:
        sys.stdout = old_stdout

# -------------------------------------------------
# 回測主計算
# -------------------------------------------------
def get_rebalancing_dates(df_prices, period):
    if period == 'never':
        return []
    df = df_prices.copy()
    df['year']  = df.index.year
    df['month'] = df.index.month
    if period == 'annually':
        dates = df.drop_duplicates(subset=['year'], keep='first').index
    elif period == 'quarterly':
        df['q'] = df.index.quarter
        dates   = df.drop_duplicates(subset=['year', 'q'], keep='first').index
    elif period == 'monthly':
        dates = df.drop_duplicates(subset=['year', 'month'], keep='first').index
    else:
        return []
    return dates[1:] if len(dates) > 1 else []

def run_simulation(port_cfg, price_data, initial_amt, benchmark_hist=None):
    tickers  = port_cfg['tickers']
    weights  = np.array(port_cfg['weights']) / 100.0
    period   = port_cfg['rebalancingPeriod']

    df_prices = price_data[tickers].copy()
    if df_prices.empty:
        return None

    port_hist = pd.Series(index=df_prices.index, dtype=float, name='value')
    rebal_dates = get_rebalancing_dates(df_prices, period)

    first_date   = df_prices.index[0]
    shares       = (initial_amt * weights) / (df_prices.loc[first_date] + EPSILON)
    port_hist.loc[first_date] = initial_amt

    for idx in range(1, len(df_prices)):
        cur_date   = df_prices.index[idx]
        cur_prices = df_prices.loc[cur_date]
        value      = (shares * cur_prices).sum()
        port_hist.loc[cur_date] = value
        if cur_date in rebal_dates:
            shares = (value * weights) / (cur_prices + EPSILON)

    port_hist.dropna(inplace=True)
    metrics = calculate_metrics(port_hist.to_frame('value'), benchmark_hist)
    return {'name': port_cfg['name'],
            **metrics,
            'portfolioHistory': [{'date': d.strftime('%Y-%m-%d'), 'value': v}
                                 for d, v in port_hist.items()]}

# -------------------------------------------------
# /api/backtest
# -------------------------------------------------
@app.post('/api/backtest')
def backtest_handler():
    try:
        data = request.get_json()
        start_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_dt    = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_str   = end_dt.strftime('%Y-%m-%d')

        all_tks = {t for p in data['portfolios'] for t in p['tickers']}
        bench   = data.get('benchmark')
        if bench:
            all_tks.add(bench)
        if not all_tks:
            return jsonify({'error': '未提供任何股票代碼'}), 400

        df_raw = download_data_silently(tuple(sorted(all_tks)), start_str, end_str)
        if isinstance(df_raw, pd.Series):
            df_raw = df_raw.to_frame(name=list(all_tks)[0])

        df_common = df_raw.dropna()
        if df_common.empty:
            return jsonify({'error': '無共同交易日'}), 400

        initial_amt = float(data['initialAmount'])

        bench_res, bench_hist = None, None
        if bench and bench in df_common.columns:
            bench_cfg = {'name': bench, 'tickers': [bench],
                         'weights': [100], 'rebalancingPeriod': 'never'}
            bench_res = run_simulation(bench_cfg, df_common, initial_amt)
            if bench_res:
                bench_hist = pd.DataFrame(bench_res['portfolioHistory'])\
                               .set_index('date').astype(float)
                bench_hist.index = pd.to_datetime(bench_hist.index)

        results = []
        for p_cfg in data['portfolios']:
            res = run_simulation(p_cfg, df_common, initial_amt, bench_hist)
            if res:
                results.append(res)

        if not results:
            return jsonify({'error': '回測失敗'}), 400

        return jsonify({'data': results, 'benchmark': bench_res})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# -------------------------------------------------
# /api/scan
# -------------------------------------------------
@app.post('/api/scan')
def scan_handler():
    try:
        data     = request.get_json()
        tickers  = data['tickers']
        bench    = data.get('benchmark')
        start    = f"{data['startYear']}-{data['startMonth']}-01"
        end_dt   = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end      = end_dt.strftime('%Y-%m-%d')

        all_dl = set(tickers)
        if bench:
            all_dl.add(bench)

        df_raw = download_data_silently(tuple(sorted(all_dl)), start, end)
        if isinstance(df_raw, pd.Series):
            df_raw = df_raw.to_frame(name=list(all_dl)[0])

        bench_hist = None
        if bench and bench in df_raw.columns:
            bench_hist = df_raw[[bench]].dropna().rename(columns={bench: 'value'})

        results = []
        for tk in tickers:
            if tk not in df_raw.columns:
                results.append({'ticker': tk, 'error': '無數據'})
                continue
            prices = df_raw[tk].dropna()
            if prices.empty:
                results.append({'ticker': tk, 'error': '無有效價格'})
                continue
            hist_df = prices.to_frame(name='value')
            metrics = calculate_metrics(hist_df, bench_hist)
            results.append({'ticker': tk, **metrics})

        return jsonify(results)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# -------------------------------------------------
# 健康檢查
# -------------------------------------------------
@app.get('/')
def root():
    return 'OK'
