// backtester_ui.js – 回測區塊 UI
import { dom }   from '../shared/dom.js';
import { state } from '../state.js';
import Chart     from 'chart.js/auto';

/* ---------------- Summary Table ---------------- */
export function renderSummaryTable(data, benchmark) {
  const tbl = dom.summaryTable;
  tbl.innerHTML = '';

  const METRICS = [
    { key: 'cagr',          label: '年化報酬率 (CAGR)' },
    { key: 'volatility',    label: '年化波動率'        },
    { key: 'mdd',           label: '最大回撤 (MDD)'    },
    { key: 'sharpe_ratio',  label: '夏普比率'          },
    { key: 'sortino_ratio', label: '索提諾比率'        },
    { key: 'beta',          label: 'Beta (β)'          },
    { key: 'alpha',         label: 'Alpha (α)'         },
    { key: 'custom_score',  label: '自訂'              }    // ★
  ];

  const FMT = {
    cagr:          v => `${(v * 100).toFixed(2)}%`,
    volatility:    v => `${(v * 100).toFixed(2)}%`,
    mdd:           v => `${(v * 100).toFixed(2)}%`,
    sharpe_ratio:  v => isFinite(v) ? v.toFixed(2) : 'N/A',
    sortino_ratio: v => isFinite(v) ? v.toFixed(2) : 'N/A',
    beta:          v => v !== null ? v.toFixed(2) : 'N/A',
    alpha:         v => v !== null ? `${(v * 100).toFixed(2)}%` : 'N/A',
    custom_score:  v => isFinite(v) ? v.toFixed(4) : 'N/A'       // ★
  };

  const thead = tbl.createTHead();
  const hRow  = thead.insertRow();
  hRow.insertCell().outerHTML = '<th class="px-4 py-2">指標</th>';
  data.forEach(p => hRow.insertCell().outerHTML =
    `<th class="px-4 py-2 text-center">${p.name}</th>`);
  if (benchmark)
    hRow.insertCell().outerHTML =
      `<th class="px-4 py-2 text-center">${benchmark.name}</th>`;

  const tbody = tbl.createTBody();
  METRICS.forEach(m => {
    const row = tbody.insertRow();
    row.insertCell().outerHTML = `<td class="px-4 py-1 font-medium">${m.label}</td>`;
    data.forEach(p => row.insertCell().outerHTML =
      `<td class="px-4 py-1 text-center">${FMT[m.key](p[m.key])}</td>`);
    if (benchmark)
      row.insertCell().outerHTML =
        `<td class="px-4 py-1 text-center">${FMT[m.key](benchmark[m.key])}</td>`;
  });
}

/* 其餘（renderGrid、renderChart 等）保持原狀 */
