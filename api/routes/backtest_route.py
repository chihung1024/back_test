from flask import Blueprint, request, jsonify
import numpy as np
from api.utils.data_handler import load_price_subset   # ← 新
from api.utils.date_tools import safe_end_date         # 你前面已新增

bp = Blueprint("backtest", __name__)

@bp.post("/api/backtest")
def backtest_api():
    p      = request.get_json()
    tickers = p["tickers"]
    start   = p["start"]
    end     = safe_end_date(p.get("end"))

    # 讀取價格  ←← 這一行替換舊的 read_parquet
    prices  = load_price_subset(tickers, start)

    # ── 以下維持原有計算 ─────────────────────────
    closes  = prices.fillna(method="ffill")
    weight  = np.repeat(1 / len(tickers), len(tickers))
    equity  = closes.dot(weight)
    ret     = equity.pct_change().dropna()

    result = {
        "start":  start,
        "end":    end,
        "cagr":   (equity.iloc[-1] / equity.iloc[0]) ** (252 / len(ret)) - 1,
        "mdd":    (equity / equity.cummax()).min() - 1,
        "sharpe": np.sqrt(252) * ret.mean() / ret.std(),
        "equity": equity.to_json(date_format="iso"),
    }
    return jsonify(result)
