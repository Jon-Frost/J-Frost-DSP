let currentDatasetId = null;
let currentDashboardId = null;
let chartType = 'bar';
let columns = [];
let charts = [];
let gridCols = 2;
const chartResizeObservers = new Map();

function getSelectedDatasetId() {
    const dsSelect = document.getElementById('datasetSelect');
    if (!dsSelect) return null;
    const dsId = dsSelect.value;
    return dsId ? dsId : null;
}

function ensureDatasetSelected() {
    if (currentDatasetId) return currentDatasetId;
    currentDatasetId = getSelectedDatasetId();
    return currentDatasetId;
}

function switchTab(tab, button) {
    document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach((c) => c.classList.remove('active'));
    if (button) button.classList.add('active');

    const target = document.getElementById('tab-' + tab);
    if (target) target.classList.add('active');
}

function selectChartType(type) {
    chartType = type;
    document.querySelectorAll('.chart-type-btn').forEach((b) => b.classList.remove('active'));
    const activeBtn = document.querySelector(`.chart-type-btn[data-type="${type}"]`);
    if (activeBtn) activeBtn.classList.add('active');

    const yGroup = document.getElementById('yColGroup');
    const aggGroup = document.getElementById('aggGroup');
    if (!yGroup || !aggGroup) return;

    if (type === 'histogram' || type === 'heatmap') {
        yGroup.style.display = 'none';
        aggGroup.style.display = 'none';
    } else {
        yGroup.style.display = 'block';
        aggGroup.style.display = 'block';
    }
}

function setGridCols(n) {
    gridCols = n;
    const grid = document.getElementById('chartGrid');
    if (!grid) return;

    grid.className = 'chart-grid';
    if (n === 1) grid.classList.add('cols-1');
    else if (n === 3) grid.classList.add('cols-3');

    document.querySelectorAll('.js-grid-btn').forEach((btn) => {
        btn.classList.toggle('active', Number(btn.dataset.gridCols) === n);
    });
}

function loadDataset() {
    const dsSelect = document.getElementById('datasetSelect');
    if (!dsSelect) return;

    const dsId = dsSelect.value;
    if (!dsId) {
        currentDatasetId = null;
        return;
    }
    currentDatasetId = dsId;

    fetch(`/api/dataset/${dsId}/preview`)
        .then((r) => r.json())
        .then((data) => {
            if (data.error) {
                showToast(data.error, 'error');
                return;
            }
            columns = data.columns;
            populateSelectors(columns);
            renderHighlights(data.highlights || []);
            renderPreview(data.preview, columns);
            renderStats(data.stats, columns);
            loadChatHistory();
        });
}

function cleanSelectedDataset() {
    const datasetId = ensureDatasetSelected();
    if (!datasetId) {
        showToast('Select a dataset first', 'warning');
        return;
    }

    const cleanBtn = document.getElementById('cleanDataBtn');
    if (cleanBtn) cleanBtn.disabled = true;

    fetch(`/api/dataset/${datasetId}/clean`, { method: 'POST' })
        .then((r) => r.json().then((data) => ({ ok: r.ok, data })))
        .then(({ ok, data }) => {
            if (!ok) {
                showToast(data.error || 'Unable to clean dataset', 'error');
                return;
            }

            showToast(data.message || 'Cleaned dataset created', 'success');
            if (data.dataset_id) {
                window.location = `/builder?dataset_id=${data.dataset_id}`;
            }
        })
        .catch((err) => {
            showToast(`Unable to clean dataset: ${err.message}`, 'error');
        })
        .finally(() => {
            if (cleanBtn) cleanBtn.disabled = false;
        });
}

function renderHighlights(highlights) {
    const container = document.getElementById('keyHighlightsContainer');
    if (!container) return;

    if (!highlights.length) {
        container.innerHTML = '<div class="empty-state highlights-empty-state"><p class="text-muted text-xs">No highlights available for this dataset</p></div>';
        return;
    }

    container.innerHTML = `
        <ul class="highlights-list">
            ${highlights.map((item) => `<li class="highlight-item">${item}</li>`).join('')}
        </ul>
    `;
}

function populateSelectors(cols) {
    const xSel = document.getElementById('xColumn');
    const ySel = document.getElementById('yColumn');
    const cSel = document.getElementById('colorColumn');
    if (!xSel || !ySel || !cSel) return;

    xSel.innerHTML = cols.map((c) => `<option value="${c.name}">${c.name} (${c.type})</option>`).join('');
    ySel.innerHTML = cols.filter((c) => c.type === 'numeric').map((c) => `<option value="${c.name}">${c.name}</option>`).join('');
    cSel.innerHTML = '<option value="">None</option>' +
        cols.filter((c) => c.type === 'categorical').map((c) => `<option value="${c.name}">${c.name}</option>`).join('');
}

function renderPreview(rows, cols) {
    if (!rows || !rows.length) return;
    const colNames = cols.map((c) => c.name);
    let html = '<div class="data-preview-table"><table>';
    html += '<thead><tr>' + colNames.map((c) => `<th>${c}</th>`).join('') + '</tr></thead>';
    html += '<tbody>';
    rows.slice(0, 30).forEach((row) => {
        html += '<tr>' + colNames.map((c) => `<td title="${row[c]}">${row[c] ?? ''}</td>`).join('') + '</tr>';
    });
    html += '</tbody></table></div>';

    const preview = document.getElementById('dataPreviewContainer');
    if (preview) preview.innerHTML = html;
}

function renderStats(stats, cols) {
    let html = '';
    cols.forEach((col) => {
        const s = stats[col.name];
        if (!s) return;
        html += `<div class="stats-card">`;
        html += `<div class="stats-card-title">${col.name} <span class="badge badge-accent">${col.type}</span></div>`;
        if (col.type === 'numeric') {
            html += `<div class="stats-values">
                Min: ${s.min?.toFixed(2) ?? 'N/A'}<br>
                Max: ${s.max?.toFixed(2) ?? 'N/A'}<br>
                Mean: ${s.mean?.toFixed(2) ?? 'N/A'}<br>
                Nulls: ${s.nulls}
            </div>`;
        } else {
            html += `<div class="stats-values">
                Unique: ${s.unique}<br>
                Top: ${s.top ?? 'N/A'}<br>
                Nulls: ${s.nulls}
            </div>`;
        }
        html += '</div>';
    });

    const statsContainer = document.getElementById('dataStatsContainer');
    if (statsContainer) {
        statsContainer.innerHTML = html || '<p class="text-muted stats-empty">No stats available</p>';
    }
}

function attachChartResizeObserver(chartId) {
    const cell = document.getElementById(chartId);
    const plotDiv = document.getElementById(chartId + '_plot');
    if (!cell || !plotDiv || chartResizeObservers.has(chartId)) return;

    const observer = new ResizeObserver(() => {
        if (!plotDiv.data || !plotDiv.layout) return;
        Plotly.Plots.resize(plotDiv);
    });

    observer.observe(cell);
    chartResizeObservers.set(chartId, observer);
}

function detachChartResizeObserver(chartId) {
    const observer = chartResizeObservers.get(chartId);
    if (!observer) return;
    observer.disconnect();
    chartResizeObservers.delete(chartId);
}

function addChart(config = null) {
    if (!ensureDatasetSelected()) {
        showToast('Select a dataset first', 'warning');
        return;
    }

    const cfg = config || {
        chart_type: chartType,
        x: document.getElementById('xColumn')?.value,
        y: document.getElementById('yColumn')?.value,
        color: document.getElementById('colorColumn')?.value,
        aggregation: document.getElementById('aggregation')?.value,
        title: document.getElementById('chartTitle')?.value,
        full_width: !!document.getElementById('fullWidthCheck')?.checked,
    };

    const chartId = 'chart_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7);
    const empty = document.getElementById('emptyCanvas');
    if (empty) empty.style.display = 'none';

    const cell = document.createElement('div');
    cell.className = 'chart-cell' + (cfg.full_width ? ' full-width' : '');
    cell.id = chartId;
    cell.innerHTML = `
        <div class="chart-actions">
            <button class="btn btn-ghost btn-sm btn-icon" type="button" data-action="toggle-full-width" data-chart-id="${chartId}" title="Toggle size">
                <i class="fas fa-expand"></i>
            </button>
            <button class="btn btn-ghost btn-sm btn-icon" type="button" data-action="remove-chart" data-chart-id="${chartId}" title="Remove">
                <i class="fas fa-times"></i>
            </button>
        </div>
        <div class="chart-plot" id="${chartId}_plot">
            <div class="chart-loading">
                <div class="spinner"></div>
            </div>
        </div>
    `;

    const grid = document.getElementById('chartGrid');
    if (!grid) return;
    grid.appendChild(cell);

    charts.push({ id: chartId, config: cfg });

    fetch('/api/chart', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dataset_id: currentDatasetId, ...cfg }),
    })
        .then((r) => r.json())
        .then((data) => {
            const plotTarget = document.getElementById(chartId + '_plot');
            if (!plotTarget) return;

            if (data.error) {
                plotTarget.innerHTML = `<div class="chart-error">${data.error}</div>`;
                return;
            }

            plotTarget.innerHTML = '';
            Plotly.newPlot(plotTarget, data.chart.data, {
                ...data.chart.layout,
                autosize: true,
            }, { responsive: true, displayModeBar: true, displaylogo: false });
            attachChartResizeObserver(chartId);
        })
        .catch((err) => {
            const plotTarget = document.getElementById(chartId + '_plot');
            if (plotTarget) {
                plotTarget.innerHTML = `<div class="chart-error">${err.message}</div>`;
            }
        });
}

function removeChart(chartId) {
    const el = document.getElementById(chartId);
    if (el) el.remove();
    detachChartResizeObserver(chartId);
    charts = charts.filter((c) => c.id !== chartId);

    if (charts.length === 0) {
        const empty = document.getElementById('emptyCanvas');
        if (empty) empty.style.display = '';
    }
}

function toggleFullWidth(chartId) {
    const cell = document.getElementById(chartId);
    if (!cell) return;

    cell.classList.toggle('full-width');
    const chart = charts.find((c) => c.id === chartId);
    if (chart) chart.config.full_width = cell.classList.contains('full-width');

    const plotDiv = document.getElementById(chartId + '_plot');
    if (plotDiv) {
        Plotly.Plots.resize(plotDiv);
    }
}

function saveDashboard() {
    if (!ensureDatasetSelected()) {
        showToast('No dataset selected', 'warning');
        return;
    }
    if (!charts.length) {
        showToast('Add at least one chart', 'warning');
        return;
    }

    const name = document.getElementById('dashboardName')?.value || 'Untitled Dashboard';
    const config = {
        charts: charts.map((c) => c.config),
        gridCols,
    };

    fetch('/api/dashboard/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name,
            dataset_id: currentDatasetId,
            config,
            dashboard_id: currentDashboardId,
        }),
    })
        .then((r) => r.json())
        .then((data) => {
            if (data.success) {
                currentDashboardId = data.dashboard_id;
                showToast('Dashboard saved!', 'success');
            }
        });
}

function loadDashboard(id) {
    fetch(`/api/dashboard/${id}`)
        .then((r) => r.json())
        .then((data) => {
            if (data.error) return;

            const nameInput = document.getElementById('dashboardName');
            if (nameInput) nameInput.value = data.name;

            currentDatasetId = data.dataset_id;
            const dsSelect = document.getElementById('datasetSelect');
            if (dsSelect) dsSelect.value = data.dataset_id;
            loadDataset();

            if (data.config.gridCols) setGridCols(data.config.gridCols);

            setTimeout(() => {
                if (data.config.charts) {
                    data.config.charts.forEach((cfg) => addChart(cfg));
                }
            }, 800);
        });
}

function exportDashboard() {
    if (!currentDashboardId) {
        saveDashboard();
        setTimeout(() => {
            if (currentDashboardId) {
                window.open(`/api/dashboard/${currentDashboardId}/export`, '_blank');
            }
        }, 1000);
        return;
    }

    window.open(`/api/dashboard/${currentDashboardId}/export`, '_blank');
}

function appendMessage(role, html, id) {
    const container = document.getElementById('chatMessages');
    if (!container) return;

    const div = document.createElement('div');
    div.className = `chat-msg ${role}`;
    if (id) div.id = id;
    div.innerHTML = html;
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
}

function formatMarkdown(text) {
    let output = text;
    output = output.replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
    output = output.replace(/`([^`]+)`/g, '<code class="chat-inline-code">$1</code>');
    output = output.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    output = output.replace(/\*(.+?)\*/g, '<em>$1</em>');
    output = output.replace(/\n/g, '<br>');
    return output;
}

function addChartFromSuggestion(cs) {
    addChart({
        chart_type: cs.chart_type || 'bar',
        x: cs.x,
        y: cs.y,
        color: cs.color || '',
        aggregation: 'sum',
        title: cs.title || '',
        full_width: false,
    });
}

function sendChat() {
    const input = document.getElementById('chatInput');
    if (!input) return;

    const msg = input.value.trim();
    if (!msg) return;
    if (!ensureDatasetSelected()) {
        showToast('Select a dataset first', 'warning');
        return;
    }

    input.value = '';
    appendMessage('user', msg);

    const typingId = 'typing_' + Date.now();
    appendMessage('assistant', '<div class="spinner spinner-inline"></div>', typingId);

    fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            dataset_id: currentDatasetId,
            message: msg,
            charts: charts.map((c) => c.config),
        }),
    })
        .then((r) => r.json())
        .then((data) => {
            const typingEl = document.getElementById(typingId);
            if (typingEl) typingEl.remove();

            if (data.error) {
                appendMessage('assistant', `<span class="text-danger">${data.error}</span>`);
                return;
            }

            let responseHtml = formatMarkdown(data.response);
            if (data.chart_suggestion) {
                const encodedChart = encodeURIComponent(JSON.stringify(data.chart_suggestion));
                responseHtml += `<br><button class="suggestion-btn js-add-suggested-chart" type="button" data-chart="${encodedChart}"><i class="fas fa-plus"></i> Add "${data.chart_suggestion.title || data.chart_suggestion.chart_type}" chart</button>`;
            }
            appendMessage('assistant', responseHtml);
        })
        .catch((err) => {
            const typingEl = document.getElementById(typingId);
            if (typingEl) typingEl.remove();
            appendMessage('assistant', `<span class="text-danger">Error: ${err.message}</span>`);
        });
}

function loadChatHistory() {
    if (!ensureDatasetSelected()) return;

    fetch(`/api/chat/history/${currentDatasetId}`)
        .then((r) => r.json())
        .then((history) => {
            const container = document.getElementById('chatMessages');
            if (!container) return;

            const welcome = container.firstElementChild;
            container.innerHTML = '';
            if (welcome) container.appendChild(welcome);

            history.forEach((h) => {
                appendMessage(h.role, formatMarkdown(h.message));
            });
        });
}

function clearChat() {
    if (!ensureDatasetSelected()) return;

    fetch(`/api/chat/clear/${currentDatasetId}`, { method: 'POST' })
        .then(() => {
            const container = document.getElementById('chatMessages');
            if (!container) return;

            const welcome = container.firstElementChild;
            container.innerHTML = '';
            if (welcome) container.appendChild(welcome);
            showToast('Chat cleared', 'success');
        });
}

function initBuilderPage() {
    const builderLayout = document.getElementById('builderLayout');
    if (builderLayout) {
        const selectedDashboardId = builderLayout.dataset.selectedDashboardId;
        currentDashboardId = selectedDashboardId ? Number(selectedDashboardId) : null;
    }

    const dsSelect = document.getElementById('datasetSelect');
    if (dsSelect) {
        dsSelect.addEventListener('change', loadDataset);
        if (dsSelect.value) loadDataset();
    }

    if (currentDashboardId) {
        loadDashboard(currentDashboardId);
    }

    document.querySelectorAll('.tab-btn').forEach((btn) => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab, btn));
    });

    document.querySelectorAll('.chart-type-btn').forEach((btn) => {
        btn.addEventListener('click', () => selectChartType(btn.dataset.type));
    });

    document.querySelectorAll('.js-grid-btn').forEach((btn) => {
        btn.addEventListener('click', () => setGridCols(Number(btn.dataset.gridCols)));
    });

    const addChartBtn = document.getElementById('addChartBtn');
    if (addChartBtn) addChartBtn.addEventListener('click', () => addChart());

    const cleanDataBtn = document.getElementById('cleanDataBtn');
    if (cleanDataBtn) cleanDataBtn.addEventListener('click', cleanSelectedDataset);

    const saveBtn = document.getElementById('saveDashboardBtn');
    if (saveBtn) saveBtn.addEventListener('click', saveDashboard);

    const exportBtn = document.getElementById('exportDashboardBtn');
    if (exportBtn) exportBtn.addEventListener('click', exportDashboard);

    const clearChatBtn = document.getElementById('clearChatBtn');
    if (clearChatBtn) clearChatBtn.addEventListener('click', clearChat);

    const sendChatBtn = document.getElementById('sendChatBtn');
    if (sendChatBtn) sendChatBtn.addEventListener('click', sendChat);

    const chatInput = document.getElementById('chatInput');
    if (chatInput) {
        chatInput.addEventListener('keypress', (event) => {
            if (event.key === 'Enter') sendChat();
        });
    }

    const toggleSidebarBtn = document.getElementById('toggleSidebarBtn');
    if (toggleSidebarBtn) {
        toggleSidebarBtn.addEventListener('click', () => {
            document.getElementById('sidebar')?.classList.toggle('mobile-show');
        });
    }

    const toggleChatBtn = document.getElementById('toggleChatBtn');
    if (toggleChatBtn) {
        toggleChatBtn.addEventListener('click', () => {
            document.getElementById('chatPanel')?.classList.toggle('mobile-show');
        });
    }

    const chartGrid = document.getElementById('chartGrid');
    if (chartGrid) {
        chartGrid.addEventListener('click', (event) => {
            const actionBtn = event.target.closest('[data-action]');
            if (!actionBtn) return;

            const chartId = actionBtn.dataset.chartId;
            if (!chartId) return;

            if (actionBtn.dataset.action === 'toggle-full-width') {
                toggleFullWidth(chartId);
            }
            if (actionBtn.dataset.action === 'remove-chart') {
                removeChart(chartId);
            }
        });
    }

    const chatMessages = document.getElementById('chatMessages');
    if (chatMessages) {
        chatMessages.addEventListener('click', (event) => {
            const btn = event.target.closest('.js-add-suggested-chart');
            if (!btn) return;

            try {
                const chart = JSON.parse(decodeURIComponent(btn.dataset.chart || ''));
                addChartFromSuggestion(chart);
            } catch {
                showToast('Unable to parse chart suggestion', 'error');
            }
        });
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initBuilderPage);
} else {
    initBuilderPage();
}
