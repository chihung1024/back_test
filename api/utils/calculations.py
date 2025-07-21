import numpy as np
import pandas as pd

# === 常數 ===
RISK_FREE_RATE        = 0.0
TRADING_DAYS_PER_YEAR = 252
DAYS_PER_YEAR         = 365.25
EPSILON               = 1e-9

def calculate_metrics(portfolio_history: pd.DataFrame,
                      benchmark_history: pd.DataFrame | None = None,
                      risk_free_rate: float = RISK_FREE_RATE) -> dict:
    """
    回傳績效指標：
    CAGR‧MDD‧Volatility‧Sharpe‧Sortino‧Beta‧Alpha‧Custom
    Custom = Sortino × Alpha × (1 + MDD)
    """
    if portfolio_history.empty or len(portfolio_history) < 2:
        return {'cagr': 0, 'mdd': 0, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    # --- 基本回測統計 ----------------------------------------------------
    end_val   = portfolio_history['value'].iloc[-1]
    start_val = portfolio_history['value'].iloc[0]
    if start_val < EPSILON:
        return {'cagr': 0, 'mdd': -1, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    years = (portfolio_history.index[-1] -
             portfolio_history.index[0]).days / DAYS_PER_YEAR
    cagr  = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    portfolio_history['peak'] = portfolio_history['value'].cummax()
    drawdown = (portfolio_history['value'] - portfolio_history['peak']) \
               / (portfolio_history['peak'] + EPSILON)
    mdd = drawdown.min()

    daily_ret = portfolio_history['value'].pct_change().dropna()
    if len(daily_ret) < 2:
        return {'cagr': cagr, 'mdd': mdd, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    vol          = daily_ret.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    excess_ret   = cagr - risk_free_rate
    sharpe_ratio = excess_ret / (vol + EPSILON)

    # Sortino
    daily_rf  = (1 + risk_free_rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
    downside  = daily_ret - daily_rf
    downside[downside > 0] = 0
    downside_std  = np.sqrt((downside ** 2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR)
    sortino_ratio = excess_ret / downside_std if downside_std > EPSILON else 0.0

    # Beta / Alpha
    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        bench_ret = benchmark_history['value'].pct_change().dropna()
        aligned   = pd.concat([daily_ret, bench_ret], axis=1, join='inner')
        aligned.columns = ['portfolio', 'benchmark']
        if len(aligned) > 1:
            cov_mat = aligned.cov()
            bench_var = cov_mat.iloc[1, 1]
            if bench_var > EPSILON:
                beta = cov_mat.iloc[0, 1] / bench_var
                bench_cagr = (benchmark_history['value'].iloc[-1] /
                              benchmark_history['value'].iloc[0]) ** (1 / years) - 1
                expected = risk_free_rate + beta * (bench_cagr - risk_free_rate)
                alpha = cagr - expected

    # 清理無效數值
    sharpe_ratio  = 0.0 if not np.isfinite(sharpe_ratio)  else sharpe_ratio
    sortino_ratio = 0.0 if not np.isfinite(sortino_ratio) else sortino_ratio
    beta  = None if beta   is not None and not np.isfinite(beta)   else beta
    alpha = None if alpha  is not None and not np.isfinite(alpha)  else alpha

    # 自訂指標
    alpha_val    = alpha if alpha is not None else 0.0
    custom_score = sortino_ratio * alpha_val * (1 + mdd)

    return {'cagr': cagr,
            'mdd': mdd,
            'volatility': vol,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'beta': beta,
            'alpha': alpha,
            'custom_score': custom_score}
