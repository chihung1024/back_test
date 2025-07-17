// handlers.js: 包含所有的事件處理函式

import { state } from './state.js';
import { dom } from './dom.js';
import * as ui from './ui.js';
import * as api from './api.js';

// --- Backtest Handlers ---

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
                payload.portfolios = []; // Invalidate payload
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

// --- Screener and Scanner Handlers ---

export async function handleRunScreener() {
    dom.screenerErrorContainer.innerHTML = '';
    const originalBtnText = dom.runScreenerBtn.innerHTML;
    dom.runScreenerBtn.innerHTML = `<div class="loader-small"></div> 正在篩選...`;
    dom.runScreenerBtn.disabled = true;

    const filters = {};
    const filterInputs = [
        { key: 'marketCap', unit: 1e8 }, { key: 'trailingPE' }, { key: 'dividendYield', unit: 0.01 },
        { key: 'returnOnEquity', unit: 0.01 }, { key: 'revenueGrowth', unit: 0.01 }, { key: 'earningsGrowth', unit: 0.01 }
    ];

    filterInputs.forEach(f => {
        const minVal = parseFloat(document.getElementById(`screener-${f.key}-min`).value);
        const maxVal = parseFloat(document.getElementById(`screener-${f.key}-max`).value);
        const unit = f.unit || 1;
        const limits = {};
        if (!isNaN(minVal)) limits.min = minVal * unit;
        if (!isNaN(maxVal)) limits.max = maxVal * unit;
        if (Object.keys(limits).length > 0) filters[f.key] = limits;
    });

    const payload = {
        index: document.getElementById('screener-index').value,
        sector: document.getElementById('screener-sector').value,
        filters: filters
    };

    const result = await api.runScreener(payload);
    if (result) {
        const newTickers = result.filter(t => !state.selectedScanTickers.includes(t));
        state.selectedScanTickers.push(...newTickers);
        ui.renderTags();
    }
    
    dom.runScreenerBtn.innerHTML = originalBtnText;
    dom.runScreenerBtn.disabled = false;
}

export async function handleRunScan() {
    dom.scanErrorContainer.innerHTML = '';
    if (state.selectedScanTickers.length === 0) {
        ui.displayError(dom.scanErrorContainer, '請至少輸入一個股票代碼。');
        return;
    }

    const payload = {
        tickers: state.selectedScanTickers,
        benchmark: document.getElementById('scan-benchmark').value.trim().toUpperCase(),
        startYear: document.getElementById('scan-startYear').value,
        startMonth: document.getElementById('scan-startMonth').value,
        endYear: document.getElementById('scan-endYear').value,
        endMonth: document.getElementById('scan-endMonth').value,
    };

    dom.scanResultsPanel.classList.remove('hidden');
    dom.scanResultsContent.classList.add('hidden');
    dom.scanLoader.classList.remove('hidden');

    try {
        const result = await api.runScan(payload);
        state.scanResultsData = result;
        handleSortScanTable(state.scanSortState.key, true);
        dom.scanResultsContent.classList.remove('hidden');
    } catch (error) {
        ui.displayError(dom.scanErrorContainer, error.message);
        dom.scanResultsPanel.classList.add('hidden');
    } finally {
        dom.scanLoader.classList.add('hidden');
    }
}

export function handleSortScanTable(key, initial = false) {
    if (!initial && state.scanSortState.key === key) {
        state.scanSortState.direction = state.scanSortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        state.scanSortState.key = key;
        state.scanSortState.direction = (['mdd', 'volatility'].includes(key)) ? 'asc' : 'desc';
    }

    state.scanResultsData.sort((a, b) => {
        if (a.error) return 1;
        if (b.error) return -1;
        const valA = a[key];
        const valB = b[key];
        if (state.scanSortState.direction === 'asc') return valA - valB;
        return valB - valA;
    });

    ui.renderScanTable(state.scanResultsData);
}


// --- Tag Input Handlers ---

export function handleTagInput(e) {
    const inputValue = dom.tagInputField.value.toUpperCase();
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp' || e.key === 'Enter') return;

    if (inputValue.length === 0) {
        dom.autocompleteSuggestions.classList.add('hidden');
        return;
    }

    const suggestions = state.allAvailableTickers.filter(t => t.startsWith(inputValue)).slice(0, 10);
    if (suggestions.length > 0) {
        dom.autocompleteSuggestions.innerHTML = suggestions.map((s, index) => `<div class="suggestion-item" data-index="${index}">${s}</div>`).join('');
        dom.autocompleteSuggestions.classList.remove('hidden');
        state.activeSuggestionIndex = -1;
    } else {
        dom.autocompleteSuggestions.classList.add('hidden');
    }
}

export function handleTagInputKeydown(e) {
    const suggestions = dom.autocompleteSuggestions.querySelectorAll('.suggestion-item');
    switch (e.key) {
        case 'Enter':
            e.preventDefault();
            if (state.activeSuggestionIndex > -1 && suggestions[state.activeSuggestionIndex]) {
                addTag(suggestions[state.activeSuggestionIndex].textContent);
            } else if (dom.tagInputField.value) {
                addTag(dom.tagInputField.value);
            }
            break;
        case 'Backspace':
            if (dom.tagInputField.value === '' && state.selectedScanTickers.length > 0) {
                removeTag(state.selectedScanTickers[state.selectedScanTickers.length - 1]);
            }
            break;
        case 'ArrowDown':
            e.preventDefault();
            if (suggestions.length > 0) {
                state.activeSuggestionIndex = (state.activeSuggestionIndex + 1) % suggestions.length;
                updateActiveSuggestion(suggestions);
            }
            break;
        case 'ArrowUp':
            e.preventDefault();
            if (suggestions.length > 0) {
                state.activeSuggestionIndex = (state.activeSuggestionIndex - 1 + suggestions.length) % suggestions.length;
                updateActiveSuggestion(suggestions);
            }
            break;
        case 'Escape':
            dom.autocompleteSuggestions.classList.add('hidden');
            break;
    }
}

function updateActiveSuggestion(suggestions) {
    suggestions.forEach((item, index) => {
        item.classList.toggle('active', index === state.activeSuggestionIndex);
    });
}

export function addTag(ticker) {
    const upperTicker = ticker.trim().toUpperCase();
    if (upperTicker && !state.selectedScanTickers.includes(upperTicker)) {
        state.selectedScanTickers.push(upperTicker);
        ui.renderTags();
    }
    dom.tagInputField.value = '';
    dom.autocompleteSuggestions.classList.add('hidden');
}

export function removeTag(ticker) {
    state.selectedScanTickers = state.selectedScanTickers.filter(t => t !== ticker);
    ui.renderTags();
}
