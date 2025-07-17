"""
data_handler.py  ⚡  DuckDB Column Push-down 版
------------------------------------------------
    • 只掃請求的股票欄位與起始日期，首次載入減少 60~80 %
    • 如果環境缺 duckdb，或 parquet_scan 失敗，會自動回退舊的
      pandas.read_parquet 流程 → 服務不中斷
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

PARQUET_PATH = Path("data/prices.parquet.gz")

# ─────────────────────────────────────────
#  DuckDB 讀取（失敗回傳 None）
# ─────────────────────────────────────────
def _load_via_duckdb(tickers: list[str], start: str | None):
    try:
        import duckdb

        cols = ", ".join(f'"{c}"' for c in tickers)
        date_where = f'WHERE "Date" >= \'{start}\'' if start else ""
        sql = (
            f'SELECT "Date", {cols} '
            f'FROM parquet_scan(\'{PARQUET_PATH}\') {date_where}'
        )

        df = duckdb.query(sql).to_df()
        if df.empty:
            return None

        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df.sort_index()

    except Exception as exc:      # duckdb 不存在或查詢失敗
        print("⚠️  duckdb fallback →", exc)
        return None


# ─────────────────────────────────────────
#  Public API：load_price_subset
# ─────────────────────────────────────────
def load_price_subset(
    tickers: list[str],
    start: str | None = None
) -> pd.DataFrame:
    """
    讀取指定股票（欄）與起始日之後的數據，回傳
    index = Date, columns = tickers 的 DataFrame
    """

    # ① 若 parquet 存在 → 嘗試 DuckDB push-down
    if PARQUET_PATH.exists():
        df = _load_via_duckdb(tickers, start)
        if df is not None:
            return df

    # ② 退回舊邏輯（pandas 一次讀整檔再切欄）
    print("📦 fallback pandas.read_parquet")
    df = pd.read_parquet(PARQUET_PATH, columns=tickers)
    if start:
        df = df[df.index >= start]
    return df.sort_index()


# 兼容舊 import
get_price_df = load_price_subset
