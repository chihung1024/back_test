from flask import Blueprint, request, jsonify
import yfinance as yf, pandas as pd, numpy as np
from api.utils.date_tools import safe_end_date

bp = Blueprint("backtest", __name__)

@bp.post("/api/backtest")
def backtest_api():
    p   = request.get_json()
    tks = p["tickers"]
    st  = p["start"]
    ed  = safe_end_date(p.get("end"))

    df = yf.download(" ".join(tks), start=st, end=ed,
                     group_by="ticker", auto_adjust=True, progress=False)

    # …以下保持原計算邏輯……………………………………
    closes = df.xs('Close', level=1, axis=1)
    weight = np.repeat(1/len(tks), len(tks))
    equity = closes.fillna(method="ffill").dot(weight)
    ret    = equity.pct_change().dropna()
    out = {
        "start": st,
        "end":   ed,
        "cagr":  (equity.iloc[-1]/equity.iloc[0]) ** (252/len(ret)) - 1,
        "mdd":   (equity/ equity.cummax()).min() - 1,
        "sharpe": np.sqrt(252)*ret.mean()/ret.std(),
        "equity": equity.to_json(date_format="iso")
    }
    return jsonify(out)
