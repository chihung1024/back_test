#!/usr/bin/env python3
"""Vectorized execution wrapper for the country momentum research script."""
from __future__ import annotations

import numpy as np
import pandas as pd

import country_momentum_backtest as cm


def simulate_fast(
    prices: pd.DataFrame,
    volumes: pd.DataFrame,
    variant: cm.Variant,
    cost_bps: float,
) -> tuple[pd.Series, pd.Series, list[dict]]:
    assets = list(cm.UNIVERSE) + ["BIL"]
    rmat = (
        prices.reindex(columns=assets)
        .pct_change(fill_method=None)
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0.0)
        .to_numpy(dtype=float)
    )

    targets, records = cm.build_targets(prices, volumes, variant)
    target_by_pos = {
        prices.index.get_loc(dt): target.reindex(assets).fillna(0.0).to_numpy(dtype=float)
        for dt, target in targets.items()
    }

    n_days, n_assets = rmat.shape
    values = np.empty(n_days, dtype=float)
    turnover = np.zeros(n_days, dtype=float)
    current_w = np.zeros(n_assets, dtype=float)
    current_w[-1] = 1.0
    wealth = 1.0
    started = False

    for i in range(n_days):
        if i in target_by_pos:
            target = target_by_pos[i]
            day_turnover = 0.5 * float(np.abs(target - current_w).sum())
            wealth *= max(0.0, 1.0 - day_turnover * cost_bps / 10_000.0)
            current_w = target.copy()
            turnover[i] = day_turnover
            started = True

        day_r = rmat[i]
        port_r = float(np.dot(current_w, day_r)) if started else float(day_r[-1])
        wealth *= 1.0 + port_r
        denom = 1.0 + port_r
        if abs(denom) > 1e-12:
            current_w *= (1.0 + day_r) / denom
        values[i] = wealth

    value_s = pd.Series(values, index=prices.index, name=variant.name).loc[cm.EVAL_START:]
    turnover_s = pd.Series(turnover, index=prices.index, name="turnover").loc[cm.EVAL_START:]
    return value_s, turnover_s, records


cm.simulate = simulate_fast
cm.main()
