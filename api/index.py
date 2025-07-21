from flask import Flask, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay, MonthEnd
import sys, os
from io import StringIO
from cachetools import cached, TTLCache
import requests

app = Flask(__name__)

# --- 全域常數 ---
RISK_FREE_RATE = 0
TRADING_DAYS_PER_YEAR = 252
DAYS_PER_YEAR = 365.25
EPSILON = 1e-9

# --- 快取設定 ---
cache = TTLCache(maxsize=128, ttl=600)

# --- 環境變數讀取 ---
GIST_RAW_URL = os.environ.get('GIST_RAW_URL')

def calculate_metrics(portfolio_history, benchmark_history=None, risk_free_rate=RISK_FREE_RATE):
    """
    計算：CAGR, MDD, Volatility, Sharpe, Sortino, Beta, Alpha, Custom
    """
    if portfolio_history.empty or len(portfolio_history) < 2:
        return {'cagr': 0, 'mdd': 0, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    end_value   = portfolio_history['value'].iloc[-1]
    start_value = portfolio_history['value'].iloc[0]
    if start_value < EPSILON:
        return {'cagr': 0, 'mdd': -1, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    start_date = portfolio_history.index[0]
    end_date   = portfolio_history.index[-1]
    years      = (end_date - start_date).days / DAYS_PER_YEAR
    cagr       = (end_value / start_value)**(1/years) - 1 if years>0 else 0

    # 最大回撤
    portfolio_history['peak']     = portfolio_history['value'].cummax()
    portfolio_history['drawdown'] = (
        portfolio_history['value'] - portfolio_history['peak']
    )/(portfolio_history['peak']+EPSILON)
    mdd = portfolio_history['drawdown'].min()

    # 日報酬率
    daily_returns = portfolio_history['value'].pct_change().dropna()
    if len(daily_returns)<2:
        return {'cagr': cagr, 'mdd': mdd, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    # 波動率 & Sharpe
    annual_std            = daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    annual_excess_return  = cagr - risk_free_rate
    sharpe_ratio          = annual_excess_return/(annual_std+EPSILON)

    # Sortino
    daily_rf   = (1+risk_free_rate)**(1/TRADING_DAYS_PER_YEAR)-1
    downside   = daily_returns - daily_rf
    downside[downside>0] = 0
    downside_std  = np.sqrt((downside**2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR)
    sortino_ratio = annual_excess_return/downside_std if downside_std>EPSILON else 0.0

    # Beta & Alpha
    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        bench_ret = benchmark_history['value'].pct_change().dropna()
        aligned   = pd.concat([daily_returns, bench_ret], axis=1, join='inner')
        aligned.columns = ['portfolio','benchmark']
        if len(aligned)>1:
            cov_mat   = aligned.cov()
            cov       = cov_mat.iloc[0,1]
            var_bench = cov_mat.iloc[1,1]
            if var_bench>EPSILON:
                beta = cov/var_bench
                be = benchmark_history['value']
                bench_cagr = (be.iloc[-1]/be.iloc[0])**(1/years)-1 if years>0 else 0
                expected = risk_free_rate + beta*(bench_cagr-risk_free_rate)
                alpha    = cagr-expected

    # 清理
    sharpe_ratio  = 0.0 if not np.isfinite(sharpe_ratio) else sharpe_ratio
    sortino_ratio = 0.0 if not np.isfinite(sortino_ratio) else sortino_ratio
    beta          = None  if beta is not None and not np.isfinite(beta) else beta
    alpha         = None  if alpha is not None and not np.isfinite(alpha) else alpha

    # 自訂指標
    alpha_val    = alpha if alpha is not None else 0.0
    custom_score = sortino_ratio * alpha_val * (1 + mdd)

    return {
        'cagr': cagr, 'mdd': mdd, 'volatility': annual_std,
        'sharpe_ratio': sharpe_ratio, 'sortino_ratio': sortino_ratio,
        'beta': beta, 'alpha': alpha, 'custom_score': custom_score
    }

def run_simulation(portfolio_config, price_data, initial_amount, benchmark_history=None):
    # ...（此處保持原邏輯不變）...
    metrics = calculate_metrics(portfolio_history.to_frame('value'), benchmark_history)
    return {
        'name': portfolio_config['name'],
        **metrics,
        'portfolioHistory': [
            {'date': date.strftime('%Y-%m-%d'), 'value': value}
            for date, value in portfolio_history.items()
        ]
    }

# 其餘 API 端點與輔助函式保持不變
# ... backtest_handler, scan_handler, screener_handler, debug_handler etc. ...
