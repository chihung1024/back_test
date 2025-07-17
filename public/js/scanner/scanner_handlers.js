// scanner_handlers.js: 只負責掃描功能的事件處理
import { state } from '../state.js';
import { dom } from '../shared/dom.js';
import * as ui from './scanner_ui.js';
import * as backtesterUi from '../backtester/backtester_ui.js';
import * as api from '../shared/api.js';

export async function handleRunScan() {
    dom.scanErrorContainer.innerHTML = '';
    if (state.selectedScanTickers.length === 0) {
        backtesterUi.displayError(dom.scanErrorContainer, '請至少輸入一個股票代碼。');
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
        backtesterUi.displayError(dom.scanErrorContainer, error.message);
        dom.scanResultsPanel.classList.add('hidden');
    } finally {
        dom.scanLoader.classList.add('hidden');
    }
}

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
    suggestions.forEach((item, index) => item.classList.toggle('active', index === state.activeSuggestionIndex));
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
