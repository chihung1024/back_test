// scanner_ui.js: 只負責掃描功能的 UI 渲染
import { dom } from '../shared/dom.js';
import { state } from '../state.js';

export function renderTags() {
    dom.tagInputContainer.querySelectorAll('.tag-item').forEach(tag => tag.remove());
    state.selectedScanTickers.slice().reverse().forEach(ticker => {
        const tagElement = document.createElement('div');
        tagElement.className = 'tag-item';
        tagElement.innerHTML = `<span>${ticker}</span><span class="tag-remove-btn" data-ticker="${ticker}">&times;</span>`;
        dom.tagInputContainer.prepend(tagElement);
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
