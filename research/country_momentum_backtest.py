#!/usr/bin/env python3
"""Country ETF cross-sectional momentum research.

This script downloads adjusted daily prices/volume with yfinance, dynamically
admits ETFs after enough live history, tests a broad but pre-declared parameter
grid, evaluates transaction-cost and subperiod robustness, and exports compact
research artifacts.

Signal timing:
- Signal is computed at a rebalance-period's final U.S. trading close.
- Target weights become effective on the next trading day.
- Therefore no same-close look-ahead is used.

This is research code, not investment advice.
"""
from __future__ import annotations

import json
import math
import time
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore", category=FutureWarning)

OUT = Path("research/results")
OUT.mkdir(parents=True, exist_ok=True)

START = "2006-01-01"
EVAL_START = "2008-01-01"
END = "2026-07-02"  # exclusive; includes 2026-06-30 and next-day execution
MIN_HISTORY = 252
MIN_MEDIAN_DOLLAR_VOLUME = 1_000_000.0
BASE_COST_BPS = 10.0

# One broad-market ETF per country. Norway uses NORW for its longer history.
UNIVERSE = {
    "ARGT": ("Argentina", "Americas"),
    "EWZ": ("Brazil", "Americas"),
    "EWC": ("Canada", "Americas"),
    "ECH": ("Chile", "Americas"),
    "COLO": ("Colombia", "Americas"),
    "EWW": ("Mexico", "Americas"),
    "EPU": ("Peru", "Americas"),
    "EWO": ("Austria", "Europe"),
    "EWK": ("Belgium", "Europe"),
    "EDEN": ("Denmark", "Europe"),
    "EFNL": ("Finland", "Europe"),
    "EWQ": ("France", "Europe"),
    "EWG": ("Germany", "Europe"),
    "GREK": ("Greece", "Europe"),
    "EIRL": ("Ireland", "Europe"),
    "EWI": ("Italy", "Europe"),
    "EWN": ("Netherlands", "Europe"),
    "NORW": ("Norway", "Europe"),
    "EPOL": ("Poland", "Europe"),
    "EWP": ("Spain", "Europe"),
    "EWD": ("Sweden", "Europe"),
    "EWL": ("Switzerland", "Europe"),
    "TUR": ("Turkey", "Europe"),
    "EWU": ("United Kingdom", "Europe"),
    "EWA": ("Australia", "Asia-Pacific"),
    "MCHI": ("China", "Asia-Pacific"),
    "EWH": ("Hong Kong", "Asia-Pacific"),
    "INDA": ("India", "Asia-Pacific"),
    "EIDO": ("Indonesia", "Asia-Pacific"),
    "EWJ": ("Japan", "Asia-Pacific"),
    "EWM": ("Malaysia", "Asia-Pacific"),
    "ENZL": ("New Zealand", "Asia-Pacific"),
    "EPHE": ("Philippines", "Asia-Pacific"),
    "EWS": ("Singapore", "Asia-Pacific"),
    "EWY": ("South Korea", "Asia-Pacific"),
    "EWT": ("Taiwan", "Asia-Pacific"),
    "THD": ("Thailand", "Asia-Pacific"),
    "VNAM": ("Vietnam", "Asia-Pacific"),
    "EIS": ("Israel", "Middle East & Africa"),
    "QAT": ("Qatar", "Middle East & Africa"),
    "KSA": ("Saudi Arabia", "Middle East & Africa"),
    "UAE": ("United Arab Emirates", "Middle East & Africa"),
    "KWT": ("Kuwait", "Middle East & Africa"),
    "EZA": ("South Africa", "Middle East & Africa"),
}
BENCHMARKS = ["ACWI", "VT", "SPY", "BIL"]
ALL_TICKERS = sorted(set(UNIVERSE) | set(BENCHMARKS))


@dataclass(frozen=True)
class Variant:
    lookback_months: int
    skip_months: int
    holdings: int
    rebalance: Literal["M", "Q", "2Q"]
    absolute_filter: Literal["none", "positive", "cash", "sma10"]
    buffer: int = 0
    weighting: Literal["equal", "inv_vol"] = "equal"
    region_cap: int = 0

    @property
    def name(self) -> str:
        return (
            f"lb{self.lookback_months}_skip{self.skip_months}_n{self.holdings}_"
            f"{self.rebalance}_abs-{self.absolute_filter}_buf{self.buffer}_"
            f"w-{self.weighting}_rc{self.region_cap}"
        )


def _field(raw: pd.DataFrame, field: str, tickers: list[str]) -> pd.DataFrame:
    """Extract one field from yfinance output across versions."""
    if isinstance(raw.columns, pd.MultiIndex):
        if field in raw.columns.get_level_values(0):
            out = raw[field].copy()
        elif field in raw.columns.get_level_values(1):
            out = raw.xs(field, axis=1, level=1).copy()
        else:
            return pd.DataFrame(index=raw.index)
    else:
        if len(tickers) == 1 and field in raw.columns:
            out = raw[[field]].copy()
            out.columns = tickers
        else:
            return pd.DataFrame(index=raw.index)
    if isinstance(out, pd.Series):
        out = out.to_frame(tickers[0])
    out.columns = [str(c) for c in out.columns]
    return out.reindex(columns=tickers)


def download_market_data(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Download in chunks, then retry missing symbols individually."""
    price_parts, vol_parts = [], []
    errors: dict[str, str] = {}
    chunk_size = 12
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        raw = None
        for attempt in range(3):
            try:
                raw = yf.download(
                    chunk,
                    start=START,
                    end=END,
                    auto_adjust=False,
                    actions=False,
                    progress=False,
                    threads=True,
                    group_by="column",
                    timeout=30,
                )
                if raw is not None and not raw.empty:
                    break
            except Exception as exc:
                errors[",".join(chunk)] = repr(exc)
            time.sleep(2 * (attempt + 1))
        if raw is None or raw.empty:
            continue
        adj = _field(raw, "Adj Close", chunk)
        if adj.empty or adj.isna().all().all():
            adj = _field(raw, "Close", chunk)
        vol = _field(raw, "Volume", chunk)
        price_parts.append(adj)
        vol_parts.append(vol)

    prices = pd.concat(price_parts, axis=1) if price_parts else pd.DataFrame()
    volumes = pd.concat(vol_parts, axis=1) if vol_parts else pd.DataFrame()
    prices = prices.loc[:, ~prices.columns.duplicated()].sort_index()
    volumes = volumes.loc[:, ~volumes.columns.duplicated()].sort_index()

    missing = [t for t in tickers if t not in prices or prices[t].dropna().empty]
    for ticker in missing:
        try:
            raw = yf.download(
                ticker,
                start=START,
                end=END,
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=False,
                timeout=30,
            )
            if raw is None or raw.empty:
                errors[ticker] = "empty"
                continue
            adj = _field(raw, "Adj Close", [ticker])
            if adj.empty or adj[ticker].dropna().empty:
                adj = _field(raw, "Close", [ticker])
            vol = _field(raw, "Volume", [ticker])
            prices[ticker] = adj[ticker]
            if ticker in vol:
                volumes[ticker] = vol[ticker]
        except Exception as exc:
            errors[ticker] = repr(exc)

    prices.index = pd.to_datetime(prices.index).tz_localize(None)
    volumes.index = pd.to_datetime(volumes.index).tz_localize(None)
    prices = prices[~prices.index.duplicated(keep="last")].sort_index()
    volumes = volumes[~volumes.index.duplicated(keep="last")].sort_index()
    prices = prices.reindex(columns=tickers)
    volumes = volumes.reindex(index=prices.index, columns=tickers)

    # Short holiday/data gaps only. Long missing stretches remain missing.
    prices = prices.ffill(limit=3)
    return prices, volumes, errors


def period_signal_dates(index: pd.DatetimeIndex, freq: str) -> list[pd.Timestamp]:
    s = pd.Series(index, index=index)
    if freq == "M":
        grp = [index.year, index.month]
    elif freq == "Q":
        grp = [index.year, index.quarter]
    elif freq == "2Q":
        grp = [index.year, np.where(index.month <= 6, 1, 2)]
    else:
        raise ValueError(freq)
    return [pd.Timestamp(x) for x in s.groupby(grp).last().values]


def rank_at(
    dt: pd.Timestamp,
    prices: pd.DataFrame,
    volumes: pd.DataFrame,
    variant: Variant,
) -> tuple[pd.Series, pd.Series]:
    loc = prices.index.get_loc(dt)
    end_loc = loc - variant.skip_months * 21
    start_loc = loc - (variant.lookback_months + variant.skip_months) * 21
    if start_loc < 0 or end_loc < 0:
        return pd.Series(dtype=float), pd.Series(dtype=bool)
    p_end = prices.iloc[end_loc]
    p_start = prices.iloc[start_loc]
    mom = p_end / p_start - 1.0

    history_count = prices.iloc[: loc + 1].notna().sum()
    dv = (prices * volumes).iloc[max(0, loc - 59): loc + 1].median()
    eligible = (
        history_count.ge(MIN_HISTORY)
        & p_end.notna()
        & p_start.notna()
        & dv.ge(MIN_MEDIAN_DOLLAR_VOLUME)
    )
    eligible = eligible.reindex(UNIVERSE.keys()).fillna(False)
    mom = mom.reindex(UNIVERSE.keys())

    if variant.absolute_filter == "positive":
        eligible &= mom.gt(0)
    elif variant.absolute_filter == "cash":
        if "BIL" in prices:
            bil_end = prices["BIL"].iloc[end_loc]
            bil_start = prices["BIL"].iloc[start_loc]
            bil_mom = bil_end / bil_start - 1.0 if pd.notna(bil_end) and pd.notna(bil_start) else 0.0
        else:
            bil_mom = 0.0
        eligible &= mom.gt(bil_mom)
    elif variant.absolute_filter == "sma10":
        sma = prices.iloc[max(0, loc - 209): loc + 1].mean()
        eligible &= prices.iloc[loc].reindex(UNIVERSE.keys()).gt(sma.reindex(UNIVERSE.keys()))
    elif variant.absolute_filter != "none":
        raise ValueError(variant.absolute_filter)

    ranked = mom[eligible].sort_values(ascending=False)
    return ranked, eligible


def choose_assets(
    ranked: pd.Series,
    current: list[str],
    variant: Variant,
) -> list[str]:
    if ranked.empty:
        return []
    rank_num = pd.Series(np.arange(1, len(ranked) + 1), index=ranked.index)
    kept: list[str] = []
    if variant.buffer > 0:
        cutoff = variant.holdings + variant.buffer
        kept = [t for t in current if t in rank_num.index and rank_num[t] <= cutoff]

    chosen = list(dict.fromkeys(kept))
    for t in ranked.index:
        if t in chosen:
            continue
        if variant.region_cap:
            region = UNIVERSE[t][1]
            if sum(UNIVERSE[x][1] == region for x in chosen) >= variant.region_cap:
                continue
        chosen.append(t)
        if len(chosen) >= variant.holdings:
            break
    return chosen[: variant.holdings]


def target_weights(
    dt: pd.Timestamp,
    chosen: list[str],
    prices: pd.DataFrame,
    variant: Variant,
) -> pd.Series:
    w = pd.Series(0.0, index=list(UNIVERSE) + ["BIL"])
    k = len(chosen)
    if k == 0:
        w["BIL"] = 1.0
        return w

    risk_budget = k / variant.holdings
    if variant.weighting == "equal":
        for t in chosen:
            w[t] = 1.0 / variant.holdings
    else:
        loc = prices.index.get_loc(dt)
        r = prices[chosen].pct_change(fill_method=None).iloc[max(0, loc - 62): loc + 1]
        vol = r.std().replace(0, np.nan)
        inv = (1.0 / vol).replace([np.inf, -np.inf], np.nan)
        if inv.notna().sum() != k:
            raw = pd.Series(1.0 / k, index=chosen)
        else:
            raw = inv / inv.sum()
        cap = min(0.35 / max(risk_budget, 1e-12), 1.0)
        raw = raw.clip(upper=cap)
        raw = raw / raw.sum()
        for t in chosen:
            w[t] = risk_budget * raw[t]
    w["BIL"] = max(0.0, 1.0 - w.sum())
    return w


def build_targets(
    prices: pd.DataFrame,
    volumes: pd.DataFrame,
    variant: Variant,
) -> tuple[dict[pd.Timestamp, pd.Series], list[dict]]:
    signals = period_signal_dates(prices.index, variant.rebalance)
    targets: dict[pd.Timestamp, pd.Series] = {}
    records: list[dict] = []
    current: list[str] = []
    for signal_dt in signals:
        loc = prices.index.get_loc(signal_dt)
        if loc + 1 >= len(prices.index):
            continue
        effective_dt = prices.index[loc + 1]
        ranked, _ = rank_at(signal_dt, prices, volumes, variant)
        chosen = choose_assets(ranked, current, variant)
        w = target_weights(signal_dt, chosen, prices, variant)
        targets[effective_dt] = w
        current = chosen
        records.append(
            {
                "signal_date": signal_dt.strftime("%Y-%m-%d"),
                "effective_date": effective_dt.strftime("%Y-%m-%d"),
                "selected": ",".join(chosen),
                "cash_weight": float(w.get("BIL", 0.0)),
                "top10": ",".join(ranked.head(10).index),
            }
        )
    return targets, records


def simulate(
    prices: pd.DataFrame,
    volumes: pd.DataFrame,
    variant: Variant,
    cost_bps: float,
) -> tuple[pd.Series, pd.Series, list[dict]]:
    assets = list(UNIVERSE) + ["BIL"]
    px = prices.reindex(columns=assets)
    rets = px.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    rets["BIL"] = rets["BIL"].fillna(0.0)

    targets, records = build_targets(prices, volumes, variant)
    wealth = 1.0
    current_w = pd.Series(0.0, index=assets)
    current_w["BIL"] = 1.0
    values = []
    turnover = []
    started = False

    for dt in prices.index:
        day_turnover = 0.0
        if dt in targets:
            target = targets[dt].reindex(assets).fillna(0.0)
            day_turnover = 0.5 * float((target - current_w).abs().sum())
            wealth *= max(0.0, 1.0 - day_turnover * cost_bps / 10_000.0)
            current_w = target
            started = True

        day_r = rets.loc[dt].reindex(assets).fillna(0.0)
        port_r = float((current_w * day_r).sum()) if started else float(day_r["BIL"])
        wealth *= 1.0 + port_r
        denom = 1.0 + port_r
        if abs(denom) > 1e-12:
            current_w = current_w * (1.0 + day_r) / denom
        values.append(wealth)
        turnover.append(day_turnover)

    value_s = pd.Series(values, index=prices.index, name=variant.name).loc[EVAL_START:]
    turnover_s = pd.Series(turnover, index=prices.index, name="turnover").loc[EVAL_START:]
    return value_s, turnover_s, records


def metrics(values: pd.Series, turnover: pd.Series | None = None) -> dict:
    values = values.dropna()
    if len(values) < 2:
        return {}
    r = values.pct_change().dropna()
    years = max((values.index[-1] - values.index[0]).days / 365.25, 1 / 365.25)
    cagr = (values.iloc[-1] / values.iloc[0]) ** (1 / years) - 1
    vol = r.std(ddof=1) * math.sqrt(252)
    sharpe = (r.mean() / r.std(ddof=1) * math.sqrt(252)) if r.std(ddof=1) > 0 else np.nan
    downside = r[r < 0]
    sortino = (r.mean() * 252 / (downside.std(ddof=1) * math.sqrt(252))) if len(downside) > 1 and downside.std(ddof=1) > 0 else np.nan
    dd = values / values.cummax() - 1
    mdd = dd.min()
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan
    monthly = values.resample("ME").last().pct_change().dropna()
    annual = values.resample("YE").last().pct_change().dropna()
    return {
        "start": values.index[0].strftime("%Y-%m-%d"),
        "end": values.index[-1].strftime("%Y-%m-%d"),
        "cagr": float(cagr),
        "volatility": float(vol),
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown": float(mdd),
        "calmar": float(calmar),
        "worst_month": float(monthly.min()) if not monthly.empty else np.nan,
        "worst_year": float(annual.min()) if not annual.empty else np.nan,
        "positive_year_rate": float((annual > 0).mean()) if not annual.empty else np.nan,
        "annual_turnover": float(turnover.sum() / years) if turnover is not None else np.nan,
    }


def subperiod_metrics(values: pd.Series) -> dict:
    periods = {
        "crisis_2008_2012": ("2008-01-01", "2012-12-31"),
        "mid_2013_2019": ("2013-01-01", "2019-12-31"),
        "recent_2020_2026": ("2020-01-01", "2026-06-30"),
        "oos_2017_2026": ("2017-01-01", "2026-06-30"),
    }
    out = {}
    for key, (a, b) in periods.items():
        sub = values.loc[a:b]
        m = metrics(sub)
        out[f"{key}_cagr"] = m.get("cagr", np.nan)
        out[f"{key}_sharpe"] = m.get("sharpe", np.nan)
        out[f"{key}_mdd"] = m.get("max_drawdown", np.nan)
    return out


def benchmark_metrics(prices: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    curves = {}
    for t in ["ACWI", "VT", "SPY"]:
        s = prices[t].dropna().loc[EVAL_START:]
        if s.empty:
            continue
        v = s / s.iloc[0]
        curves[t] = v
        rows.append({"variant": t, **metrics(v), **subperiod_metrics(v)})
    return pd.DataFrame(rows), pd.DataFrame(curves)


def make_grid() -> list[Variant]:
    variants: dict[str, Variant] = {}
    for lb in [3, 6, 9, 12]:
        for skip in [0, 1]:
            for n in [3, 5, 8, 10]:
                for rb in ["M", "Q", "2Q"]:
                    for af in ["none", "positive", "cash"]:
                        v = Variant(lb, skip, n, rb, af)
                        variants[v.name] = v

    for lb in [3, 6, 9, 12]:
        for skip in [0, 1]:
            for af in ["none", "positive", "cash", "sma10"]:
                for buffer in [0, 3]:
                    for weighting in ["equal", "inv_vol"]:
                        for region_cap in [0, 2]:
                            v = Variant(lb, skip, 5, "Q", af, buffer, weighting, region_cap)
                            variants[v.name] = v
    return list(variants.values())


def robust_score(df: pd.DataFrame) -> pd.Series:
    """Predeclared rank aggregation; avoids selecting only the best CAGR."""
    high_good = [
        "sharpe", "calmar", "cagr", "oos_2017_2026_sharpe",
        "recent_2020_2026_sharpe", "crisis_2008_2012_mdd",
    ]
    weights = {
        "sharpe": 0.20,
        "calmar": 0.15,
        "cagr": 0.15,
        "oos_2017_2026_sharpe": 0.20,
        "recent_2020_2026_sharpe": 0.10,
        "crisis_2008_2012_mdd": 0.15,
        "annual_turnover": 0.05,
    }
    score = pd.Series(0.0, index=df.index)
    for c in high_good:
        score += weights[c] * df[c].rank(pct=True, ascending=True).fillna(0.0)
    score += weights["annual_turnover"] * df["annual_turnover"].rank(pct=True, ascending=False).fillna(0.0)
    return score


def main() -> None:
    prices, volumes, errors = download_market_data(ALL_TICKERS)
    available = [t for t in ALL_TICKERS if t in prices and prices[t].notna().sum() > 0]
    if "BIL" not in available:
        prices["BIL"] = 1.0
        volumes["BIL"] = np.inf

    country_cols = [t for t in UNIVERSE if t in prices]
    prices = prices.loc[prices[country_cols].notna().any(axis=1)].copy()
    volumes = volumes.reindex(index=prices.index, columns=prices.columns)
    prices.to_csv(OUT / "downloaded_adjusted_prices.csv.gz", compression="gzip")
    volumes.to_csv(OUT / "downloaded_volume.csv.gz", compression="gzip")

    variants = make_grid()
    result_rows = []
    signals_by_variant: dict[str, list[dict]] = {}

    for i, variant in enumerate(variants, 1):
        values, to, records = simulate(prices, volumes, variant, BASE_COST_BPS)
        row = {"variant": variant.name, **asdict(variant)}
        row.update(metrics(values, to))
        row.update(subperiod_metrics(values))
        result_rows.append(row)
        signals_by_variant[variant.name] = records
        if i % 50 == 0:
            print(f"completed {i}/{len(variants)}")

    results = pd.DataFrame(result_rows)
    results["robust_score"] = robust_score(results)
    results = results.sort_values(["robust_score", "sharpe"], ascending=False).reset_index(drop=True)

    practical = results[
        (results["holdings"] == 5)
        & (results["rebalance"] == "Q")
        & (results["weighting"] == "equal")
    ].copy()
    practical["practical_score"] = robust_score(practical)
    practical = practical.sort_values(["practical_score", "sharpe"], ascending=False)

    recommended_name = practical.iloc[0]["variant"]
    recommended_row = practical.iloc[0].to_dict()
    recommended_variant = next(v for v in variants if v.name == recommended_name)

    cost_rows = []
    rec_curve = None
    rec_turnover = None
    rec_records = None
    for bps in [0, 10, 25, 50]:
        values, to, records = simulate(prices, volumes, recommended_variant, bps)
        cost_rows.append({"cost_bps": bps, **metrics(values, to), **subperiod_metrics(values)})
        if bps == 10:
            rec_curve, rec_turnover, rec_records = values, to, records

    baseline = Variant(6, 0, 5, "Q", "none", 0, "equal", 0)
    base_values, base_to, _ = simulate(prices, volumes, baseline, BASE_COST_BPS)

    sop_candidate = Variant(6, 0, 5, "Q", "cash", 3, "equal", 0)
    sop_values, sop_to, _ = simulate(prices, volumes, sop_candidate, BASE_COST_BPS)

    bench_rows, bench_curves = benchmark_metrics(prices)
    curves = {
        "Original_6M_Top5_Q": base_values,
        "Prior_SOP_6M_Cash_Buffer": sop_values,
        "Robust_Selected": rec_curve,
    }
    for c in bench_curves.columns:
        curves[c] = bench_curves[c]
    curve_df = pd.DataFrame(curves).dropna(how="all")

    comparison = pd.DataFrame(
        [
            {"variant": "Original_6M_Top5_Q", **metrics(base_values, base_to), **subperiod_metrics(base_values)},
            {"variant": "Prior_SOP_6M_Cash_Buffer", **metrics(sop_values, sop_to), **subperiod_metrics(sop_values)},
            {"variant": "Robust_Selected", **metrics(rec_curve, rec_turnover), **subperiod_metrics(rec_curve)},
        ]
    )
    comparison = pd.concat([comparison, bench_rows], ignore_index=True, sort=False)

    latest = rec_records[-1] if rec_records else {}
    current_selected = latest.get("selected", "").split(",") if latest.get("selected") else []
    latest_signal = pd.Timestamp(latest["signal_date"]) if latest else prices.index[-1]
    latest_rank, _ = rank_at(latest_signal, prices, volumes, recommended_variant)
    current_w = target_weights(latest_signal, current_selected, prices, recommended_variant)
    current_rows = []
    for t, weight in current_w[current_w > 1e-9].items():
        current_rows.append(
            {
                "signal_date": latest.get("signal_date"),
                "effective_date": latest.get("effective_date"),
                "ticker": t,
                "country": "Cash / T-bills" if t == "BIL" else UNIVERSE[t][0],
                "region": "Defensive" if t == "BIL" else UNIVERSE[t][1],
                "weight": float(weight),
                "momentum_rank": int(latest_rank.index.get_loc(t) + 1) if t in latest_rank.index else np.nan,
                "lookback_return": float(latest_rank.get(t, np.nan)),
            }
        )

    yearly = curve_df.resample("YE").last().pct_change()
    yearly.index = yearly.index.year
    yearly.index.name = "year"

    results.to_csv(OUT / "variant_grid.csv", index=False)
    practical.to_csv(OUT / "practical_top5_quarterly.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(OUT / "cost_sensitivity.csv", index=False)
    comparison.to_csv(OUT / "strategy_comparison.csv", index=False)
    curve_df.to_csv(OUT / "equity_curves.csv")
    yearly.to_csv(OUT / "yearly_returns.csv")
    pd.DataFrame(current_rows).to_csv(OUT / "current_signal.csv", index=False)
    pd.DataFrame(
        [
            {
                "ticker": t,
                "country": UNIVERSE[t][0],
                "region": UNIVERSE[t][1],
                "first_valid_date": prices[t].first_valid_index().strftime("%Y-%m-%d") if prices[t].first_valid_index() else None,
                "last_valid_date": prices[t].last_valid_index().strftime("%Y-%m-%d") if prices[t].last_valid_index() else None,
                "observations": int(prices[t].notna().sum()),
            }
            for t in UNIVERSE
        ]
    ).to_csv(OUT / "universe_coverage.csv", index=False)

    summary = {
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "data_start_requested": START,
        "evaluation_start": EVAL_START,
        "data_end_exclusive": END,
        "actual_first_date": prices.index.min().strftime("%Y-%m-%d"),
        "actual_last_date": prices.index.max().strftime("%Y-%m-%d"),
        "available_tickers": available,
        "download_errors": errors,
        "variant_count": len(variants),
        "base_cost_bps": BASE_COST_BPS,
        "eligibility": {
            "minimum_history_days": MIN_HISTORY,
            "minimum_60d_median_dollar_volume": MIN_MEDIAN_DOLLAR_VOLUME,
            "one_etf_per_country": True,
        },
        "original_baseline": {
            "parameters": asdict(baseline),
            "metrics": metrics(base_values, base_to),
        },
        "prior_sop_candidate": {
            "parameters": asdict(sop_candidate),
            "metrics": metrics(sop_values, sop_to),
        },
        "recommended": {
            "parameters": asdict(recommended_variant),
            "metrics_at_10bps": metrics(rec_curve, rec_turnover),
            "robust_selection_row": recommended_row,
            "latest_signal": latest,
            "current_positions": current_rows,
        },
        "limitations": [
            "Expanded universe is based mainly on currently listed ETFs, so residual survivorship bias remains.",
            "Yahoo adjusted-price conventions and occasional missing/delisted symbols can affect results.",
            "Taxes, Taiwan brokerage/custody fees, FX conversion costs, and market impact are not modeled.",
            "Ranking results are not a forecast and may suffer momentum reversals.",
        ],
    }
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    report = f"""# Country ETF Momentum Research Result

Generated: {summary['generated_utc']}

## Robust-selected practical rule

```json
{json.dumps(asdict(recommended_variant), indent=2)}
```

Base assumption: {BASE_COST_BPS:.0f} bps one-way turnover cost.

Latest signal:
```json
{json.dumps(latest, indent=2)}
```

See CSV/JSON artifacts in this directory for the full grid and robustness tables.
"""
    (OUT / "README.md").write_text(report, encoding="utf-8")
    print(json.dumps(summary["recommended"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
