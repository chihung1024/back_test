import { dom } from '../shared/dom.js';
import { state } from '../state.js';

export function renderTags() { /* 原功能保持不變 */ }

export function renderScanTable(results) {
  const table = dom.scanSummaryTable;
  table.innerHTML = '';

  // 新增「自訂」
  const metrics = [
    { key: 'cagr',          label: '年化報酬率 (CAGR)' },
    { key: 'volatility',    label: '年化波動率'        },
    { key: 'mdd',           label: '最大回撤 (MDD)'    },
    { key: 'sharpe_ratio',  label: '夏普比率'          },
    { key: 'sortino_ratio', label: '索提諾比率'        },
    { key: 'beta',          label: 'Beta (β)'          },
    { key: 'alpha',         label: 'Alpha (α)'         },
    { key: 'custom_score',  label: '自訂'              }   // ★
  ];

  const fmt = {
    cagr:          v => `${(v * 100).toFixed(2)}%`,
    volatility:    v => `${(v * 100).toFixed(2)}%`,
    mdd:           v => `${(v * 100).toFixed(2)}%`,
    sharpe_ratio:  v => isFinite(v) ? v.toFixed(2) : 'N/A',
    sortino_ratio: v => isFinite(v) ? v.toFixed(2) : 'N/A',
    beta:          v => v !== null ? v.toFixed(2)   : 'N/A',
    alpha:         v => v !== null ? `${(v * 100).toFixed(2)}%` : 'N/A',
    custom_score:  v => isFinite(v) ? v.toFixed(4) : 'N/A'       // ★
  };

  /* thead */
  const thead = table.createTHead();
  const hRow  = thead.insertRow();
  hRow.className = 'bg-gray-100 text-sm font-medium';
  hRow.insertCell().outerHTML =
    `<th class="px-4 py-2 sortable" data-sort-key="ticker">Ticker</th>`;
  metrics.forEach(m => {
    hRow.insertCell().outerHTML =
      `<th class="px-4 py-2 sortable" data-sort-key="${m.key}">${m.label}</th>`;
  });

  /* tbody */
  const tbody = table.createTBody();
  results.forEach(r => {
    const row = tbody.insertRow();
    row.insertCell().outerHTML =
      `<td class="px-4 py-1 font-semibold">${r.ticker}${r.note||''}</td>`;
    metrics.forEach(m => {
      const val = r[m.key];
      row.insertCell().outerHTML =
        `<td class="px-4 py-1 text-center">${
          val!==undefined ? fmt[m.key](val) : '—'
        }</td>`;
    });
  });
}
