// ui.js: 包含所有與渲染和更新 UI 相關的函式

import { dom } from './dom.js';
import { state, setChartInstance } from './state.js';

export function populateDateSelectors(startYId, startMId, endYId, endMId) {
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

export function renderGrid() {
    const gridHead = dom.portfolioGrid.querySelector('thead');
    const gridBody = dom.portfolioGrid.querySelector('tbody');
    const gridFoot = dom.portfolioGrid.querySelector('tfoot');
    
    gridHead.innerHTML = '';
    gridBody.innerHTML = '';
    gridFoot.innerHTML = '';

    const headerRow = gridHead.insertRow();
    headerRow.insertCell().outerHTML = `<th class="sticky left-0 bg-gray-50 z-10"><div class="flex items-center justify-center space-x-2"><span>股票代碼</span><button class="clear-tickers-btn text-gray-400 hover:text-red-500 p-1 rounded-full" title="清除所有資產代碼"><svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg></button></div></th>`;
    state.portfolios.forEach(p => {
        const th = document.createElement('th');
        th.className = 'min-w-[150px]';
        th.innerHTML = `<div class="flex items-center justify-center space-x-2"><input type="text" value="${p.name}" class="portfolio-name-input font-bold text-center bg-transparent flex-grow" data-old-name="${p.name}"><button class="clear-portfolio-btn text-gray-400 hover:text-red-500 p-1 rounded-full" title="清除此組合權重" data-portfolio-name="${p.name}"><svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg></button></div>`;
        headerRow.appendChild(th);
    });
    headerRow.insertCell().outerHTML = `<th></th>`;

    state.assets.forEach((asset, assetIndex) => {
        const row = gridBody.insertRow();
        row.insertCell().outerHTML = `<td class="sticky left-0 bg-white"><input type="text" value="${asset.ticker}" class="ticker-input uppercase" data-asset-index="${assetIndex}"></td>`;
        state.portfolios.forEach(p => {
            const weight = state.weights[assetIndex]?.[p.name] || 0;
            row.insertCell().innerHTML = `<input type="number" min="0" max="100" value="${weight}" class="weight-input" data-asset-index="${assetIndex}" data-portfolio-name="${p.name}">`;
        });
        row.insertCell().innerHTML = `<button class="remove-asset-btn text-red-400 hover:text-red-600 font-bold text-xl" data-asset-index="${assetIndex}">&times;</button>`;
    });

    const footerRow = gridFoot.insertRow();
    footerRow.className = "font-bold bg-gray-50";
    footerRow.insertCell().textContent = '總計';
    state.portfolios.forEach(() => footerRow.insertCell());
    footerRow.insertCell();
    updateTotals();
}

export function updateTotals() {
    const totalCells = dom.portfolioGrid.querySelectorAll('tfoot td:not(:first-child):not(:last-child)');
    state.portfolios.forEach((p, index) => {
        const total = state.weights.reduce((sum, weightRow) => sum + (weightRow[p.name] || 0), 0);
        const cell = totalCells[index];
        if (cell) {
            cell.textContent = `${total.toFixed(0)}%`;
            cell.className = total === 100 ? 'text-green-600' : 'text-red-600';
        }
    });
}

export function displayError(container, message) {
    container.innerHTML = `<div class="error-message"><strong>錯誤：</strong> ${message}</div>`;
}

export function renderTags() {
    dom.tagInputContainer.querySelectorAll('.tag-item').forEach(tag => tag.remove());
    state.selectedScanTickers.slice().reverse().forEach(ticker => {
        const tagElement = document.createElement('div');
        tagElement.className = 'tag-item';
        tagElement.innerHTML = `<span>${ticker}</span><span class="tag-remove-btn" data-ticker="${ticker}">&times;</span>`;
        dom.tagInputContainer.prepend(tagElement);
    });
}

export function renderChart(portfolios, benchmark) {
    const ctx = dom.portfolioChartCanvas.getContext('2d');
    const datasets = portfolios.map((p, i) => ({
        label: p.name, data: p.portfolioHistory, borderColor: state.COLORS[i % state.COLORS.length],
        borderWidth: 2, pointRadius: 0, fill: false, tension: 0.1,
        parsing: { xAxisKey: 'date', yAxisKey: 'value' }
    }));

    if (benchmark) {
        datasets.push({
            label: `${benchmark.name} (基準)`, data: benchmark.portfolioHistory,
            borderColor: '#374151', borderDash: [5, 5], borderWidth: 2,
            pointRadius: 0, fill: false, tension: 0.1,
            parsing: { xAxisKey: 'date', yAxisKey: 'value' }
        });
    }

    if (state.chartInstance) state.chartInstance.destroy();
    const newChart = new Chart(ctx, {
        type: 'line', data: { datasets }, options: {
            responsive: true, maintainAspectRatio: false,
            scales: { x: { type: 'time', time: { unit: 'year' } }, y: { type: 'logarithmic', ticks: { callback: (value) => '$' + value.toLocaleString() } } },
            plugins: { tooltip: { mode: 'index', intersect: false, itemSort: (a, b) => b.parsed.y - a.parsed.y, callbacks: { label: (context) => { let label = context.dataset.label || ''; if (label) label += ': '; if (context.parsed.y !== null) { label += new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(context.parsed.y); } return label; } } } }
        }
    });
    setChartInstance(newChart);
}

export function renderSummaryTable(portfolios, benchmark) {
    const table = dom.summaryTable;
    table.innerHTML = '';
    const metrics = [ 
        { key: 'cagr', label: '年化報酬率 (CAGR)', format: (v) => `${(v * 100).toFixed(2)}%` }, 
        { key: 'volatility', label: '年化波動率', format: (v) => `${(v * 100).toFixed(2)}%` },
        { key: 'mdd', label: '最大回撤 (MDD)', format: (v) => `${(v * 100).toFixed(2)}%` }, 
        { key: 'sharpe_ratio', label: '夏普比率', format: (v) => isFinite(v) ? v.toFixed(2) : 'N/A' }, 
        { key: 'sortino_ratio', label: '索提諾比率', format: (v) => isFinite(v) ? v.toFixed(2) : 'N/A' },
        { key: 'beta', label: 'Beta (β)', format: (v) => v !== null ? v.toFixed(2) : 'N/A' },
        { key: 'alpha', label: 'Alpha (α)', format: (v) => v !== null ? `${(v * 100).toFixed(2)}%` : 'N/A' }
    ];
    const thead = table.createTHead(); const headerRow = thead.insertRow(); headerRow.className = "bg-gray-100";
    headerRow.insertCell().outerHTML = `<th class="text-left pl-2">指標</th>`;
    portfolios.forEach(p => headerRow.insertCell().outerHTML = `<th>${p.name}</th>`);
    if (benchmark) { headerRow.insertCell().outerHTML = `<th class="bg-gray-200">${benchmark.name} (基準)</th>`; }
    
    const tbody = table.createTBody();
    metrics.forEach(metric => {
        const row = tbody.insertRow();
        row.insertCell().outerHTML = `<td class="font-semibold text-left pl-2">${metric.label}</td>`;
        portfolios.forEach(p => {
            const cell = row.insertCell();
            cell.textContent = metric.format(p[metric.key]);
            if (['cagr', 'sharpe_ratio', 'sortino_ratio', 'alpha'].includes(metric.key)) cell.className = 'text-green-600 font-medium';
            if (['mdd', 'volatility'].includes(metric.key)) cell.className = 'text-red-600 font-medium';
        });
        if (benchmark) {
            const cell = row.insertCell();
            cell.textContent = metric.format(benchmark[metric.key]);
            cell.className = "bg-gray-50";
        }
    });
}

export function renderScanTable(results) {
    const table = dom.scanSummaryTable;
    table.innerHTML = '';
    const metrics = [
        { key: 'cagr', label: '年化報酬率 (CAGR)'}, { key: 'volatility', label: '年化波動率'},
        { key: 'mdd', label: '最大回撤 (MDD)'}, { key: 'sharpe_ratio', label: '夏普比率'},
        { key: 'sortino_ratio', label: '索提諾比率'}, { key: 'beta', label: 'Beta (β)'},
        { key: 'alpha', label: 'Alpha (α)'}
    ];
    const formatters = {
        cagr: (v) => `${(v * 100).toFixed(2)}%`, volatility: (v) => `${(v * 100).toFixed(2)}%`,
        mdd: (v) => `${(v * 100).toFixed(2)}%`, sharpe_ratio: (v) => isFinite(v) ? v.toFixed(2) : 'N/A',
        sortino_ratio: (v) => isFinite(v) ? v.toFixed(2) : 'N/A', beta: (v) => v !== null ? v.toFixed(2) : 'N/A',
        alpha: (v) => v !== null ? `${(v * 100).toFixed(2)}%` : 'N/A'
    };

    const thead = table.createTHead(); const headerRow = thead.insertRow(); headerRow.className = "bg-gray-100";
    let headerHTML = `<th class="text-left pl-2">股票代碼</th>`;
    metrics.forEach(m => {
        let sortClass = '';
        if (state.scanSortState.key === m.key) {
            sortClass = state.scanSortState.direction === 'asc' ? 'sort-asc' : 'sort-desc';
        }
        headerHTML += `<th class="sortable ${sortClass}" data-sort-key="${m.key}">${m.label}</th>`;
    });
    headerRow.innerHTML = headerHTML;

    const tbody = table.createTBody();
    results.forEach(res => {
        const row = tbody.insertRow();
        let tickerHTML = res.ticker;
        if (res.note) { tickerHTML += ` <span class="text-xs text-gray-500 font-normal">${res.note}</span>`; }
        row.insertCell().outerHTML = `<td class="font-semibold text-left pl-2">${tickerHTML}</td>`;
        metrics.forEach(metric => {
            const cell = row.insertCell();
            if (res.error) {
                cell.textContent = res.error;
                cell.className = 'text-gray-500';
            } else {
                cell.textContent = formatters[metric.key](res[metric.key]);
                if (['cagr', 'sharpe_ratio', 'sortino_ratio', 'alpha'].includes(metric.key)) cell.className = 'text-green-600 font-medium';
                if (['mdd', 'volatility'].includes(metric.key)) cell.className = 'text-red-600 font-medium';
            }
        });
    });
}
