import pandas as pd
from pandas.tseries.offsets import BDay

_US_TZ = "US/Eastern"

def prev_trading_day(ts: pd.Timestamp | None = None) -> pd.Timestamp:
    """回傳傳入日期的『上一個美股營業日』(含時區)。"""
    if ts is None:
        ts = pd.Timestamp.now(tz=_US_TZ).normalize()
    if ts.tzinfo is None:
        ts = ts.tz_localize(_US_TZ)
    return (ts - BDay(1)).normalize()

def safe_end_date(raw: str | None) -> str:
    """
    只要 end >= 今天(美東) 或為空，就改成『上一個收盤日』字串。
    其餘保持原值（字串）。
    """
    if raw in ("", None):
        return prev_trading_day().strftime("%Y-%m-%d")

    end = pd.to_datetime(raw)
    if end.tzinfo is None:
        end = end.tz_localize(_US_TZ)
    if end.normalize() >= pd.Timestamp.now(tz=_US_TZ).normalize():
        end = prev_trading_day(end)
    return end.strftime("%Y-%m-%d")
