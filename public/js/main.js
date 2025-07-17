// main.js: 應用程式的主入口點
import { state } from './state.js';
import { dom } from './shared/dom.js';
import * as api from './shared/api.js';
import * as backtesterUI from './backtester/backtester_ui.js';
import * as scannerHandlers from './scanner/scanner_handlers.js';
import { handleRunBacktest } from './backtester/backtester_handlers.js';

function populateDateSelectors(startYId, startMId, endYId, endMId) {
    const currentYear = new Date().getFullYear();
    const startYearSelect = document.getElementById(startYId);
    const endYearSelect = document.getElementById(endYId);
    const startMonthSelect = document.getElementById(startMId);
    const endMonthSelect = document.getElementById(endMId);
    for (let year = currentYear; year >= 1980; year--) {
        startYearSelect.add(new Option(year, year));
        endYearSelect.add(new Option(year, year));
    }
    startYearSelect.value = 2015;
    endYearSelect.value = currentYear;
    for (let month = 1; month <= 12; month++) {
        startMonthSelect.add(new Option(month, month));
        endMonthSelect.add(new Option(month, month));
    }
    startMonthSelect.value = 1;
    endMonthSelect.value = new Date().getMonth() + 1;
}

function initializeTabs() {
    const tabsContainer = document.getElementById('main-tabs-container');
    const tabPanels = document.querySelectorAll('.tab-panel');
    tabsContainer.addEventListener('click', (e) => {
        e.preventDefault();
        const clickedTab = e.target.closest('.tab-link');
        if (!clickedTab) return;
        tabsContainer.querySelectorAll('.tab-link').forEach(tab => {
            tab.classList.remove('active', 'text-indigo-600', 'border-indigo-600');
            tab.classList.add('text-gray-500', 'hover:text-gray-700', 'hover:border-gray-300');
        });
        clickedTab.classList.add('active', 'text-indigo-600', 'border-indigo-600');
        const targetPanelId = clickedTab.dataset.tab + '-panel';
        tabPanels.forEach(panel => panel.classList.toggle('hidden', panel.id !== targetPanelId));
    });
}

function initializeBacktesterListeners() {
    dom.runBacktestBtn.addEventListener('click', handleRunBacktest);
    document.getElementById('add-asset-btn').addEventListener('click', () => {
        state.assets.push({ ticker: '' });
        const newWeightRow = {};
        state.portfolios.forEach(p => newWeightRow[p.name] = 0);
        state.weights.push(newWeightRow);
        backtesterUI.renderGrid();
    });
    document.getElementById('add-portfolio-btn').addEventListener('click', () => {
        if (state.portfolios.length >= 5) {
            alert('最多只能比較 5 組投資組合。');
            return;
        }
        const newName = `投組 ${state.portfolios.length + 1}`;
        state.portfolios.push({ name: newName });
        state.weights.forEach(weightRow => { weightRow[newName] = 0; });
        backtesterUI.renderGrid();
    });
    dom.portfolioGrid.addEventListener('input', (e) => {
        const target = e.target;
        if (target.classList.contains('weight-input')) {
            state.weights[target.dataset.assetIndex][target.dataset.portfolioName] = parseFloat(target.value) || 0;
            backtesterUI.updateTotals();
        }
        if (target.classList.contains('ticker-input')) {
            state.assets[target.dataset.assetIndex].ticker = target.value.toUpperCase();
        }
    });
    dom.portfolioGrid.addEventListener('change', (e) => {
        const target = e.target;
        if (target.classList.contains('portfolio-name-input')) {
            const oldName = target.dataset.oldName;
            const newName = target.value;
            if (oldName === newName) return;
            const portfolioToUpdate = state.portfolios.find(p => p.name === oldName);
            if (portfolioToUpdate) portfolioToUpdate.name = newName;
            state.weights.forEach(weightRow => {
                if (weightRow[oldName] !== undefined) {
                    weightRow[newName] = weightRow[oldName];
                    delete weightRow[oldName];
                }
            });
            backtesterUI.renderGrid();
        }
    });
    dom.portfolioGrid.addEventListener('click', (e) => {
        const removeAssetBtn = e.target.closest('.remove-asset-btn');
        if (removeAssetBtn) {
            state.assets.splice(parseInt(removeAssetBtn.dataset.assetIndex), 1);
            state.weights.splice(parseInt(removeAssetBtn.dataset.assetIndex), 1);
            backtesterUI.renderGrid();
        }
        const clearPortfolioBtn = e.target.closest('.clear-portfolio-btn');
        if (clearPortfolioBtn) {
            const portfolioToClear = clearPortfolioBtn.dataset.portfolioName;
            state.weights.forEach(weightRow => {
                if (weightRow[portfolioToClear] !== undefined) weightRow[portfolioToClear] = 0;
            });
            backtesterUI.renderGrid();
        }
        const clearTickersBtn = e.target.closest('.clear-tickers-btn');
        if (clearTickersBtn) {
            state.assets.forEach(asset => { asset.ticker = ''; });
            backtesterUI.renderGrid();
        }
    });
}

function initializeScannerListeners() {
    dom.runScanBtn.addEventListener('click', scannerHandlers.handleRunScan);
    dom.runScreenerBtn.addEventListener('click', scannerHandlers.handleRunScreener);
    dom.tagInputField.addEventListener('keyup', scannerHandlers.handleTagInput);
    dom.tagInputField.addEventListener('keydown', scannerHandlers.handleTagInputKeydown);
    dom.tagInputContainer.addEventListener('click', () => dom.tagInputField.focus());
    dom.autocompleteSuggestions.addEventListener('click', (e) => {
        if (e.target.classList.contains('suggestion-item')) {
            scannerHandlers.addTag(e.target.textContent);
        }
    });
    document.addEventListener('click', (e) => {
        if (!dom.tagInputContainer.contains(e.target)) {
            dom.autocompleteSuggestions.classList.add('hidden');
        }
    });
    dom.scanSummaryTable.addEventListener('click', (e) => {
        const th = e.target.closest('.sortable');
        if (th) {
            scannerHandlers.handleSortScanTable(th.dataset.sortKey);
        }
    });
}

async function initialize() {
    populateDateSelectors('startYear', 'startMonth', 'endYear', 'endMonth');
    populateDateSelectors('scan-startYear', 'scan-startMonth', 'scan-endYear', 'scan-endMonth');
    backtesterUI.renderGrid();
    initializeBacktesterListeners();
    initializeScannerListeners();
    initializeTabs();
    state.allAvailableTickers = await api.fetchAvailableTickers();
}

initialize();
