// backtester_handlers.js: 只負責回測功能的事件處理
import { state } from '../state.js';
import { dom } from '../shared/dom.js';
import * as ui from './backtester_ui.js';
import * as api from '../shared/api.js';

export async function handleRunBacktest() {
    dom.errorContainer.innerHTML = '';
    dom.warningContainer.innerHTML = '';
    
    const payload = {
        initialAmount: parseFloat(document.getElementById('initialAmount').value),
        startYear: document.getElementById('startYear').value,
        startMonth: document.getElementById('startMonth').value,
        endYear: document.getElementById('endYear').value,
        endMonth: document.getElementById('endMonth').value,
        rebalancingPeriod: document.getElementById('rebalancingPeriod').value,
        benchmark: document.getElementById('benchmark').value.trim().toUpperCase(),
        portfolios: []
    };

    state.portfolios.forEach(p => {
        const portfolioConfig = { name: p.name, tickers: [], weights: [], rebalancingPeriod: payload.rebalancingPeriod };
        state.assets.forEach((asset, index) => {
            const weight = state.weights[index]?.[p.name] || 0;
            if (weight > 0 && asset.ticker) {
                portfolioConfig.tickers.push(asset.ticker);
                portfolioConfig.weights.push(weight);
            }
        });
        if (portfolioConfig.tickers.length > 0) {
            const totalWeight = portfolioConfig.weights.reduce((a, b) => a + b, 0);
            if (Math.abs(totalWeight - 100) > 0.01) {
                ui.displayError(dom.errorContainer, `投資組合 "${p.name}" 的總權重不為 100%，請修正。`);
                payload.portfolios = [];
                return;
            }
            payload.portfolios.push(portfolioConfig);
        }
    });

    if (payload.portfolios.length === 0 && !dom.errorContainer.innerHTML) {
        ui.displayError(dom.errorContainer, '請至少設定一個有效的投資組合 (總權重為100%，且股票代碼不為空)。');
        return;
    }
    if (dom.errorContainer.innerHTML) return;

    dom.resultsPanel.classList.remove('hidden');
    dom.resultsContent.classList.add('hidden');
    dom.loader.classList.remove('hidden');

    try {
        const result = await api.runBacktest(payload);
        if (result.warning) {
            dom.warningContainer.innerHTML = `<div class="warning-message" role="alert"><p class="font-bold">請注意</p><p>${result.warning}</p></div>`;
        }
        ui.renderSummaryTable(result.data, result.benchmark);
        ui.renderChart(result.data, result.benchmark);
        dom.resultsContent.classList.remove('hidden');
    } catch (error) {
        ui.displayError(dom.errorContainer, error.message);
        dom.resultsPanel.classList.add('hidden');
    } finally {
        dom.loader.classList.add('hidden');
    }
}
