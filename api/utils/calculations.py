import numpy as np
import pandas as pd

# --- 全域常數 ---
RISK_FREE_RATE         = 0.0
TRADING_DAYS_PER_YEAR  = 252
DAYS_PER_YEAR          = 365.25
EPSILON                = 1e-9

def calculate_metrics(portfolio_history: pd.DataFrame,
                      benchmark_history: pd.DataFrame | None = None,
                      risk_free_rate: float = RISK_FREE_RATE) -> dict:
    """
    回傳績效指標：
    CAGR‧MDD‧Volatility‧Sharpe‧Sortino‧Beta‧Alpha‧Custom (Sortino*Alpha*(1+MDD))
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

    cagr = (end_value / start_value) ** (1 / years) - 1 if years > 0 else 0

    portfolio_history['peak']      = portfolio_history['value'].cummax()
    portfolio_history['drawdown']  = (portfolio_history['value'] -
                                      portfolio_history['peak']) / (portfolio_history['peak'] + EPSILON)
    mdd = portfolio_history['drawdown'].min()

    daily_returns = portfolio_history['value'].pct_change().dropna()
    if len(daily_returns) < 2:
        return {'cagr': cagr, 'mdd': mdd, 'volatility': 0,
                'sharpe_ratio': 0, 'sortino_ratio': 0,
                'beta': None, 'alpha': None, 'custom_score': 0}

    annual_std              = daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    annualized_excess_ret   = cagr - risk_free_rate
    sharpe_ratio            = annualized_excess_ret / (annual_std + EPSILON)

    daily_rf_rate   = (1 + risk_free_rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
    downside        = daily_returns - daily_rf_rate
    downside[downside > 0] = 0
    downside_std    = np.sqrt((downside**2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR)
    sortino_ratio   = annualized_excess_ret / downside_std if downside_std > EPSILON else 0.0

    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        bench_ret = benchmark_history['value'].pct_change().dropna()
        aligned   = pd.concat([daily_returns, bench_ret], axis=1, join='inner')
        aligned.columns = ['portfolio', 'benchmark']
        if len(aligned) > 1:
            cov_matrix         = aligned.cov()
            covariance         = cov_matrix.iloc[0, 1]
            benchmark_variance = cov_matrix.iloc[1, 1]
            if benchmark_variance > EPSILON:
                beta = covariance / benchmark_variance
                bench_end = benchmark_history['value'].iloc[-1]
                bench_st  = benchmark_history['value'].iloc[0]
                bench_cagr = (bench_end / bench_st) ** (1 / years) - 1 if years > 0 else 0
                expected_ret = risk_free_rate + beta * (bench_cagr - risk_free_rate)
                alpha = cagr - expected_ret

    # 清理非數值
    sharpe_ratio  = 0.0 if not np.isfinite(sharpe_ratio)  else sharpe_ratio
    sortino_ratio = 0.0 if not np.isfinite(sortino_ratio) else sortino_ratio
    beta          = None if beta   is not None and not np.isfinite(beta)   else beta
    alpha         = None if alpha  is not None and not np.isfinite(alpha)  else alpha

    # --- 自訂指標 ----------------------------------------------------------
    alpha_val   = alpha if alpha is not None else 0.0
    custom_score = sortino_ratio * alpha_val * (1 + mdd)
    # ----------------------------------------------------------------------

    return {'cagr': cagr, 'mdd': mdd, 'volatility': annual_std,
            'sharpe_ratio': sharpe_ratio, 'sortino_ratio': sortino_ratio,
            'beta': beta, 'alpha': alpha, 'custom_score': custom_score}
