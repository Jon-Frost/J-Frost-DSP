// ══════════════════════════════════════════════════════════════════════════════
// EXPORT RENDERING — PARSE EMBEDDED CHART JSON AND RENDER WITH PLOTLY
// ══════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    // FIND THE CHART GRID AND HIDDEN JSON TEMPLATE ELEMENT
    const grid = document.getElementById('chartGrid');
    const jsonTemplate = document.getElementById('chartsDataJson');
    if (!grid || !jsonTemplate) return;

    // PARSE THE EMBEDDED CHART DATA FROM THE SCRIPT TAG
    let chartsData = [];
    try {
        chartsData = JSON.parse((jsonTemplate.innerHTML || '[]').trim());
    } catch {
        chartsData = [];
    }

    // CREATE A PLOTLY CHART FOR EACH EXPORTED CHART CONFIG
    chartsData.forEach((chart, i) => {
        const div = document.createElement('div');
        div.className = 'export-chart-cell';
        div.id = 'export_chart_' + i;
        grid.appendChild(div);

        Plotly.newPlot(div.id, chart.data, {
            ...chart.layout,
            autosize: true,
            height: 320,
        }, { responsive: true, displayModeBar: true, displaylogo: false });
    });
});
