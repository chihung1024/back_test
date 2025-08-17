from flask import Flask, request, jsonify
import yfinance as yf
import pandas as pd
import numpy as np
from pandas.tseries.offsets import BDay, MonthEnd
import sys, os, time
from io import StringIO
from pathlib import Path
from cachetools import cached, TTLCache
import requests

app = Flask(__name__)

# ── 全域常數 ───────────────────────────────────────────
RISK_FREE_RATE = 0
TRADING_DAYS_PER_YEAR = 252
DAYS_PER_YEAR = 365.25
EPSILON = 1e-9

# ── 快取 ──────────────────────────────────────────────
cache = TTLCache(maxsize=128, ttl=600)  # 10 分鐘

# ── 環境變數 ──────────────────────────────────────────
GIST_RAW_URL = os.environ.get("GIST_RAW_URL")

# ── 本地 Parquet（由 GitHub Action 更新）──────────────
PARQUET_FILE = Path("data/prices.parquet.gz")

# ── ── ── 輔助：載入本地快取價格 ──────────────────────
@cached(cache)
def load_cached_prices() -> pd.DataFrame | None:
    """
    讀取 GitHub Action 預先匯出的歷史收盤價 Parquet。
    回傳 DataFrame，rows 為日期，columns 為 ticker；若檔案不存在回傳 None。
    """
    if PARQUET_FILE.exists():
        return pd.read_parquet(PARQUET_FILE)
    return None


# ── ── ── 計算績效指標 ────────────────────────────────
def calculate_metrics(portfolio_history, benchmark_history=None, risk_free_rate=RISK_FREE_RATE):
    if portfolio_history.empty or len(portfolio_history) < 2:
        return {"cagr": 0, "mdd": 0, "volatility": 0, "sharpe_ratio": 0, "sortino_ratio": 0, "beta": None, "alpha": None}

    end_value = portfolio_history["value"].iloc[-1]
    start_value = portfolio_history["value"].iloc[0]
    if start_value < EPSILON:
        return {"cagr": 0, "mdd": -1, "volatility": 0, "sharpe_ratio": 0, "sortino_ratio": 0, "beta": None, "alpha": None}

    start_date = portfolio_history.index[0]
    end_date = portfolio_history.index[-1]
    years = (end_date - start_date).days / DAYS_PER_YEAR
    cagr = (end_value / start_value) ** (1 / years) - 1 if years > 0 else 0

    portfolio_history["peak"] = portfolio_history["value"].cummax()
    portfolio_history["drawdown"] = (portfolio_history["value"] - portfolio_history["peak"]) / (portfolio_history["peak"] + EPSILON)
    mdd = portfolio_history["drawdown"].min()

    daily_returns = portfolio_history["value"].pct_change().dropna()
    if len(daily_returns) < 2:
        return {"cagr": cagr, "mdd": mdd, "volatility": 0, "sharpe_ratio": 0, "sortino_ratio": 0, "beta": None, "alpha": None}

    annual_std = daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)
    annualized_excess_return = cagr - risk_free_rate
    sharpe_ratio = annualized_excess_return / (annual_std + EPSILON)

    daily_risk_free_rate = (1 + risk_free_rate) ** (1 / TRADING_DAYS_PER_YEAR) - 1
    downside_returns = daily_returns - daily_risk_free_rate
    downside_returns[downside_returns > 0] = 0
    downside_std = np.sqrt((downside_returns**2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR)
    sortino_ratio = annualized_excess_return / downside_std if downside_std > EPSILON else 0.0

    beta, alpha = None, None
    if benchmark_history is not None and not benchmark_history.empty:
        benchmark_returns = benchmark_history["value"].pct_change().dropna()
        aligned_returns = pd.concat([daily_returns, benchmark_returns], axis=1, join="inner")
        aligned_returns.columns = ["portfolio", "benchmark"]
        if len(aligned_returns) > 1:
            cov_matrix = aligned_returns.cov()
            covariance = cov_matrix.iloc[0, 1]
            bench_var = cov_matrix.iloc[1, 1]
            if bench_var > EPSILON:
                beta = covariance / bench_var
                bench_end_val = benchmark_history["value"].iloc[-1]
                bench_start_val = benchmark_history["value"].iloc[0]
                bench_cagr = (bench_end_val / bench_start_val) ** (1 / years) - 1 if years > 0 else 0
                expected_return = risk_free_rate + beta * (bench_cagr - risk_free_rate)
                alpha = cagr - expected_return

    # 清洗無限與 NaN
    for k in ("sharpe_ratio", "sortino_ratio"):
        if not np.isfinite(locals()[k]) or np.isnan(locals()[k]):
            locals()[k] = 0.0
    for k in ("beta", "alpha"):
        if locals()[k] is not None and (not np.isfinite(locals()[k]) or np.isnan(locals()[k])):
            locals()[k] = None

    return {
        "cagr": cagr,
        "mdd": mdd,
        "volatility": annual_std,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "beta": beta,
        "alpha": alpha,
    }


# ── ── ── 下載（或本地讀取）股價 ───────────────────────
@cached(cache)
def download_data_silently(tickers, start_date, end_date):
    """
    從 yfinance 批次下載 Close 價 (自動調整)。傳入 tickers 為 tuple。
    透過 stdout 重導避免 noisy progress bar。
    """
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        dfs = []
        for i in range(0, len(tickers), 15):
            part = yf.download(
                list(tickers[i : i + 15]),
                start=start_date,
                end=end_date,
                auto_adjust=True,
                progress=False,
            )["Close"]
            dfs.append(part)
        data = pd.concat(dfs, axis=1)
    finally:
        sys.stdout = old_stdout
    return data


def get_price_data(tickers, start_date, end_date) -> pd.DataFrame:
    """
    先讀 Parquet，若缺資料或檔案異常則回退 yfinance。
    """
    need_dl = set(tickers)
    parts = []

    # 1. 嘗試載入本地快照
    cached_df = load_cached_prices()
    if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
        print(">>> 使用 Parquet 快取") 
        # 確保索引能比較
        idx = cached_df.index
        if not isinstance(idx, pd.DatetimeIndex):
            idx = pd.to_datetime(idx, errors="coerce")
        cached_df = cached_df.set_index(idx)      # ← 複製後重新賦值，避開 read-only
        cached_df = cached_df[~cached_df.index.isna()]

        mask = (cached_df.index >= start_date) & (cached_df.index <= end_date)
        subset = cached_df.loc[mask]

        present = need_dl & set(subset.columns)
        if present:
            parts.append(subset[present])
            need_dl -= present  # 剩下才往 yfinance 抓
            
    # 2. 下載不足部分
    if need_dl:
        print(f">>> 下載 yfinance：{need_dl}")
        dl = download_data_silently(tuple(need_dl), start_date, end_date)
        if isinstance(dl, pd.Series):
            dl = dl.to_frame()
        parts.append(dl)

    # 3. 合併結果
    return pd.concat(parts, axis=1).sort_index() if parts else pd.DataFrame()




# ── ── ── 回測核心 ───────────────────────────────────────
def get_rebalancing_dates(df_prices, period):
    if period == "never":
        return []
    df = df_prices.copy()
    df["year"] = df.index.year
    df["month"] = df.index.month
    if period == "annually":
        rebal_dates = df.drop_duplicates(subset=["year"], keep="first").index
    elif period == "quarterly":
        df["quarter"] = df.index.quarter
        rebal_dates = df.drop_duplicates(subset=["year", "quarter"], keep="first").index
    elif period == "monthly":
        rebal_dates = df.drop_duplicates(subset=["year", "month"], keep="first").index
    else:
        return []
    return rebal_dates[1:] if len(rebal_dates) > 1 else []


def validate_data_completeness(df_prices_raw, all_tickers, requested_start_date):
    problematic = []
    for tk in all_tickers:
        if tk in df_prices_raw.columns:
            first_valid = df_prices_raw[tk].first_valid_index()
            if first_valid is not None and first_valid > requested_start_date + BDay(5):
                problematic.append({"ticker": tk, "start_date": first_valid.strftime("%Y-%m-%d")})
    return problematic


def run_simulation(portfolio_config, price_data, initial_amount, benchmark_history=None):
    tickers = portfolio_config["tickers"]
    weights = np.array(portfolio_config["weights"]) / 100.0
    rebalancing_period = portfolio_config["rebalancingPeriod"]

    df_prices = price_data[tickers].copy()
    if df_prices.empty:
        return None

    portfolio_history = pd.Series(index=df_prices.index, dtype=float, name="value")
    rebal_dates = get_rebalancing_dates(df_prices, rebalancing_period)

    current_date = df_prices.index[0]
    shares = (initial_amount * weights) / (df_prices.loc[current_date] + EPSILON)
    portfolio_history.loc[current_date] = initial_amount

    for i in range(1, len(df_prices)):
        current_date = df_prices.index[i]
        current_value = (shares * df_prices.loc[current_date]).sum()
        portfolio_history.loc[current_date] = current_value
        if current_date in rebal_dates:
            shares = (current_value * weights) / (df_prices.loc[current_date] + EPSILON)

    portfolio_history.dropna(inplace=True)
    metrics = calculate_metrics(portfolio_history.to_frame("value"), benchmark_history)
    history_pairs = [{"date": d.strftime("%Y-%m-%d"), "value": v} for d, v in portfolio_history.items()]
    return {"name": portfolio_config["name"], **metrics, "portfolioHistory": history_pairs}


# ── ── ── API：/api/backtest ──────────────────────────────
@app.route("/api/backtest", methods=["POST"])
def backtest_handler():
    try:
        data = request.get_json()
        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime("%Y-%m-%d")

        all_tickers = {tk for p in data["portfolios"] for tk in p["tickers"]}
        benchmark_tk = data.get("benchmark")
        if benchmark_tk:
            all_tickers.add(benchmark_tk)
        if not all_tickers:
            return jsonify({"error": "請至少在一個投資組合中設定一項資產。"}), 400

        price_df = get_price_data(tuple(sorted(all_tickers)), start_date_str, end_date_str)
        if price_df.empty or price_df.isnull().all().any():
            return jsonify({"error": "無法取得完整價格資料。"}), 400

        warn = None
        probs = validate_data_completeness(price_df, all_tickers, pd.to_datetime(start_date_str))
        if probs:
            tk_str = ", ".join([f"{p['ticker']} (從 {p['start_date']} 開始)" for p in probs])
            warn = f"部分資產的數據起始日晚於您的選擇。回測已自動調整至最早的共同可用日期：{tk_str}"

        price_df = price_df.dropna()
        if price_df.empty:
            return jsonify({"error": "在指定的時間範圍內找不到共同交易日。"}), 400

        initial_amount = float(data["initialAmount"])
        benchmark_result, bench_history = None, None
        if benchmark_tk and benchmark_tk in price_df.columns:
            bench_conf = {"name": benchmark_tk, "tickers": [benchmark_tk], "weights": [100], "rebalancingPeriod": "never"}
            benchmark_result = run_simulation(bench_conf, price_df, initial_amount)
            if benchmark_result:
                bench_history = (
                    pd.DataFrame(benchmark_result["portfolioHistory"]).set_index("date").astype({"value": float})
                )
                bench_history.index = pd.to_datetime(bench_history.index)

        results = []
        for p_conf in data["portfolios"]:
            if not p_conf["tickers"]:
                continue
            res = run_simulation(p_conf, price_df, initial_amount, bench_history)
            if res:
                results.append(res)

        if not results:
            return jsonify({"error": "沒有足夠的共同交易日來進行回測。"}), 400

        if benchmark_result:
            benchmark_result["beta"] = 1.0
            benchmark_result["alpha"] = 0.0
            benchmark_result.update(calculate_metrics(bench_history))

        return jsonify({"data": results, "benchmark": benchmark_result, "warning": warn})
    except Exception as e:
        import traceback

        print(traceback.format_exc())
        return jsonify({"error": f"伺服器錯誤: {e}"}), 500


# ── ── ── API：/api/scan ─────────────────────────────────
@app.route("/api/scan", methods=["POST"])
def scan_handler():
    try:
        data = request.get_json()
        tickers = data["tickers"]
        benchmark_tk = data.get("benchmark")
        if not tickers:
            return jsonify({"error": "股票代碼列表不可為空。"}), 400

        start_date_str = f"{data['startYear']}-{data['startMonth']}-01"
        end_date = pd.to_datetime(f"{data['endYear']}-{data['endMonth']}-01") + MonthEnd(0)
        end_date_str = end_date.strftime("%Y-%m-%d")

        all_tk = set(tickers)
        if benchmark_tk:
            all_tk.add(benchmark_tk)

        price_df = get_price_data(tuple(sorted(all_tk)), start_date_str, end_date_str)
        if price_df.empty:
            return jsonify({"error": "無法取得價格資料"}), 400

        bench_hist = None
        if benchmark_tk and benchmark_tk in price_df.columns:
            bench_prices = price_df[[benchmark_tk]].dropna()
            if not bench_prices.empty:
                bench_hist = bench_prices.rename(columns={benchmark_tk: "value"})

        results = []
        req_start = pd.to_datetime(start_date_str)
        for tk in tickers:
            if tk not in price_df.columns:
                results.append({"ticker": tk, "error": "找不到數據"})
                continue

            series = price_df[tk].dropna()
            if series.empty:
                results.append({"ticker": tk, "error": "指定範圍內無數據"})
                continue

            note = None
            prob = validate_data_completeness(price_df, [tk], req_start)
            if prob:
                note = f"(從 {prob[0]['start_date']} 開始)"

            metrics = calculate_metrics(series.to_frame("value"), bench_hist)
            results.append({"ticker": tk, **metrics, "note": note})

        return jsonify(results)
    except Exception as e:
        import traceback

        print(traceback.format_exc())
        return jsonify({"error": f"伺服器錯誤: {e}"}), 500


# ── ── ── API：/api/screener ────────────────────────────
@cached(cache)
def get_preprocessed_data():
    if not GIST_RAW_URL:
        raise ValueError("GIST_RAW_URL 環境變數未設定")
    resp = requests.get(GIST_RAW_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


@app.route("/api/screener", methods=["POST"])
def screener_handler():
    try:
        data = request.get_json()
        index = data.get("index", "sp500")
        min_mc = data.get("minMarketCap", 0)
        sector = data.get("sector", "any")

        all_stocks = get_preprocessed_data()
        if index == "sp500":
            base = [s for s in all_stocks if s.get("in_sp500")]
        elif index == "nasdaq100":
            base = [s for s in all_stocks if s.get("in_nasdaq100")]
        elif index == "russell3000":
            base = [s for s in all_stocks if s.get("in_russell3000")]
        else:
            base = all_stocks

        filt = []
        for s in base:
            if s.get("marketCap", 0) < min_mc:
                continue
            if sector != "any" and s.get("sector") != sector:
                continue
            filt.append(s["ticker"])
        return jsonify(filt)
    except ValueError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        import traceback

        print(traceback.format_exc())
        return jsonify({"error": f"篩選器錯誤: {e}"}), 500


# ── ── ── 其他 ──────────────────────────────────────────
@app.route("/", methods=["GET"])
@app.route("/api/debug")
def debug_handler():
    return jsonify({k: v for k, v in os.environ.items()})


def index():
    return "Python backend is running."


# ── ── ── Main ──────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
