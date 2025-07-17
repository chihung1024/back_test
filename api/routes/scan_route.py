from flask import Blueprint, request, jsonify
import yfinance as yf

bp = Blueprint("scan", __name__)

@bp.post("/api/scan")
def scan_api():
    tk = request.json["ticker"].strip().upper()

    # 只抓 Close 欄，避免欄位數浮動
    hist = yf.download(tk, period="5y", auto_adjust=True,
                       progress=False)[["Close"]]
    if hist.empty:
        return jsonify({"error": "no data"}), 404

    hist.reset_index(inplace=True)
    hist.columns = ["Date", "Close"]

    info = yf.Ticker(tk).info
    resp = {
        "ticker": tk,
        "marketCap": info.get("marketCap"),
        "pe": info.get("trailingPE"),
        "price": hist["Close"].iloc[-1],
        "history": hist.to_dict(orient="records")
    }
    return jsonify(resp)
